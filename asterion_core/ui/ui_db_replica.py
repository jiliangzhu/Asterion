from __future__ import annotations

import dataclasses
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from asterion_core.storage.logger import get_logger


log = get_logger(__name__)

DEFAULT_UI_DB_REPLICA_PATH = "data/ui/asterion_ui.duckdb"


def default_ui_db_replica_path() -> str:
    return os.getenv("ASTERION_UI_DB_REPLICA_PATH", DEFAULT_UI_DB_REPLICA_PATH)


def default_ui_replica_meta_path(*, replica_db_path: str | None = None) -> str:
    env_path = os.getenv("ASTERION_UI_REPLICA_META_PATH", "").strip()
    if env_path:
        return env_path
    db_path = Path(replica_db_path or default_ui_db_replica_path())
    return str(db_path.with_name(f"{db_path.stem}.meta.json"))


def load_ui_replica_meta(meta_path: str) -> dict[str, Any] | None:
    path = Path(meta_path)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def _validate_duckdb_file(db_path: Path) -> None:
    code = (
        "import sys\n"
        "import duckdb\n"
        "p = sys.argv[1]\n"
        "con = duckdb.connect(p, read_only=True)\n"
        "try:\n"
        "    con.execute(\"SELECT COUNT(*) FROM information_schema.tables\").fetchone()\n"
        "finally:\n"
        "    con.close()\n"
    )
    try:
        proc = subprocess.run(
            [sys.executable, "-c", code, str(db_path)],
            check=True,
            capture_output=True,
            timeout=30,
        )
    except FileNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Python interpreter not found for replica validation") from exc
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or b"").decode("utf-8", errors="ignore").strip()
        if not err:
            err = (exc.stdout or b"").decode("utf-8", errors="ignore").strip()
        raise RuntimeError(err or "duckdb_validate_failed") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("duckdb_validate_timeout") from exc
    if proc.returncode != 0:
        raise RuntimeError("duckdb_validate_failed")


def _safe_stat(path: Path) -> dict[str, int | None]:
    if not path.exists():
        return {"size_bytes": None, "mtime_ms": None}
    st = path.stat()
    return {
        "size_bytes": int(st.st_size),
        "mtime_ms": int(st.st_mtime * 1000),
    }


def _copy_file_fast(src: Path, dst: Path) -> None:
    if dst.exists():
        dst.unlink()
    copy_mode = os.getenv("ASTERION_UI_REPLICA_COPY_MODE", "auto").strip().lower()
    clone_timeout_s = max(1, int(os.getenv("ASTERION_UI_REPLICA_CLONE_TIMEOUT_S", "8")))
    auto_copy_max_bytes = max(
        1,
        int(os.getenv("ASTERION_UI_REPLICA_AUTO_COPY_MAX_BYTES", str(512 * 1024 * 1024))),
    )

    def _clone_copy() -> None:
        if sys.platform == "darwin":
            subprocess.run(["cp", "-c", str(src), str(dst)], check=True, capture_output=True, timeout=clone_timeout_s)
            return
        if sys.platform.startswith("linux"):
            subprocess.run(
                ["cp", "--reflink=always", str(src), str(dst)],
                check=True,
                capture_output=True,
                timeout=clone_timeout_s,
            )
            return
        raise RuntimeError(f"clone_copy_unsupported_platform:{sys.platform}")

    if copy_mode == "copy":
        shutil.copy2(src, dst)
        return
    if copy_mode == "clone":
        try:
            _clone_copy()
            return
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"clone_copy_failed: {exc}") from exc
    if copy_mode == "auto":
        try:
            _clone_copy()
            return
        except Exception:
            if int(src.stat().st_size) <= auto_copy_max_bytes:
                shutil.copy2(src, dst)
                return
            raise RuntimeError("clone_copy_failed_large_source")
    raise RuntimeError(f"unsupported_copy_mode:{copy_mode}")


def _sync_file_rsync(src: Path, dst: Path) -> None:
    timeout_s = int(os.getenv("ASTERION_UI_REPLICA_RSYNC_TIMEOUT_S", "0"))
    cmd = ["rsync", "-a", "--no-whole-file", "--partial", "--delay-updates", str(src), str(dst)]
    try:
        kwargs: dict[str, Any] = {"check": True, "capture_output": True}
        if timeout_s > 0:
            kwargs["timeout"] = max(1, timeout_s)
        subprocess.run(cmd, **kwargs)
    except subprocess.CalledProcessError as exc:
        err = (exc.stderr or b"").decode("utf-8", errors="ignore").strip()
        if not err:
            err = (exc.stdout or b"").decode("utf-8", errors="ignore").strip()
        raise RuntimeError(f"rsync_sync_failed: rc={exc.returncode} err={err[:400]}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"rsync_sync_failed: timeout after {timeout_s}s") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"rsync_sync_failed: {exc}") from exc


def _create_local_snapshot(src: Path) -> Path:
    snapshot = src.with_name(f".{src.name}.ui_snapshot_{os.getpid()}_{int(time.time() * 1000)}")
    try:
        if sys.platform == "darwin":
            subprocess.run(["cp", "-c", str(src), str(snapshot)], check=True, capture_output=True, timeout=30)
        elif sys.platform.startswith("linux"):
            subprocess.run(["cp", "--reflink=auto", str(src), str(snapshot)], check=True, capture_output=True, timeout=30)
        else:
            shutil.copy2(src, snapshot)
    except Exception:
        shutil.copy2(src, snapshot)
    return snapshot


@dataclasses.dataclass(frozen=True)
class ReplicaRefreshResult:
    ok: bool
    ts_ms: int
    error: str | None
    src_db_path: str
    dst_db_path: str
    meta_path: str
    elapsed_ms: int


def refresh_ui_db_replica_once(
    *,
    src_db_path: str,
    dst_db_path: str,
    meta_path: str | None = None,
    refresh_interval_s: float | None = None,
) -> ReplicaRefreshResult:
    started_ms = int(time.time() * 1000)
    src = Path(src_db_path)
    dst = Path(dst_db_path)
    meta = Path(meta_path or default_ui_replica_meta_path(replica_db_path=str(dst)))
    prev = load_ui_replica_meta(str(meta)) or {}

    def _emit(ok: bool, error: str | None) -> ReplicaRefreshResult:
        now_ms = int(time.time() * 1000)
        src_stat = _safe_stat(src)
        dst_stat = _safe_stat(dst)
        payload = {
            "source_db_path": str(src),
            "replica_db_path": str(dst),
            "last_attempt_ts_ms": now_ms,
            "last_success_ts_ms": now_ms if ok else prev.get("last_success_ts_ms"),
            "consecutive_failures": 0 if ok else int(prev.get("consecutive_failures", 0) or 0) + 1,
            "last_error": None if ok else str(error),
            "source_size_bytes": src_stat.get("size_bytes"),
            "source_mtime_ms": src_stat.get("mtime_ms"),
            "replica_size_bytes": dst_stat.get("size_bytes"),
            "replica_mtime_ms": dst_stat.get("mtime_ms"),
            "refresh_interval_s": float(refresh_interval_s) if refresh_interval_s is not None else prev.get("refresh_interval_s"),
        }
        _write_json_atomic(meta, payload)
        return ReplicaRefreshResult(
            ok=ok,
            ts_ms=now_ms,
            error=error,
            src_db_path=str(src),
            dst_db_path=str(dst),
            meta_path=str(meta),
            elapsed_ms=max(0, now_ms - started_ms),
        )

    tmp = Path(str(dst) + ".tmp")
    max_copy_attempts = max(1, int(os.getenv("ASTERION_UI_REPLICA_MAX_COPY_ATTEMPTS", "3")))
    try:
        if not src.exists():
            raise FileNotFoundError(f"source db not found: {src}")

        src_stat_before = _safe_stat(src)
        if (
            dst.exists()
            and prev.get("last_error") in (None, "")
            and int(prev.get("consecutive_failures", 0) or 0) == 0
            and prev.get("source_mtime_ms") == src_stat_before.get("mtime_ms")
            and prev.get("source_size_bytes") == src_stat_before.get("size_bytes")
        ):
            return _emit(True, None)

        dst.parent.mkdir(parents=True, exist_ok=True)
        src_dev = int(src.resolve().stat().st_dev)
        if dst.exists():
            dst_dev = int(dst.resolve().stat().st_dev)
        else:
            dst_dev = int(dst.parent.resolve().stat().st_dev)
        cross_device = src_dev != dst_dev
        if cross_device:
            snap = _create_local_snapshot(src)
            try:
                _sync_file_rsync(snap, dst)
            finally:
                if snap.exists():
                    snap.unlink()
            _validate_duckdb_file(dst)
            return _emit(True, None)

        if tmp.exists():
            tmp.unlink()
        copied = False
        for _ in range(max_copy_attempts):
            src_before = _safe_stat(src)
            _copy_file_fast(src, tmp)
            src_after = _safe_stat(src)
            if (
                src_before.get("size_bytes") == src_after.get("size_bytes")
                and src_before.get("mtime_ms") == src_after.get("mtime_ms")
            ):
                copied = True
                break
            if tmp.exists():
                tmp.unlink()
        if not copied:
            raise RuntimeError("source_db_changed_during_copy")

        _validate_duckdb_file(tmp)
        os.replace(tmp, dst)
        return _emit(True, None)
    except Exception as exc:  # noqa: BLE001
        if tmp.exists():
            tmp.unlink()
        return _emit(False, str(exc))


def run_ui_db_replica_loop(
    *,
    src_db_path: str,
    dst_db_path: str,
    meta_path: str | None = None,
    interval_s: float = 5.0,
) -> None:
    while True:
        result = refresh_ui_db_replica_once(
            src_db_path=src_db_path,
            dst_db_path=dst_db_path,
            meta_path=meta_path,
            refresh_interval_s=interval_s,
        )
        if result.ok:
            log.info(
                "ui replica refresh ok src=%s dst=%s elapsed_ms=%s",
                result.src_db_path,
                result.dst_db_path,
                result.elapsed_ms,
            )
        else:
            log.warning(
                "ui replica refresh failed src=%s dst=%s elapsed_ms=%s err=%s",
                result.src_db_path,
                result.dst_db_path,
                result.elapsed_ms,
                result.error,
            )
        time.sleep(max(0.5, float(interval_s)))


__all__ = [
    "DEFAULT_UI_DB_REPLICA_PATH",
    "ReplicaRefreshResult",
    "default_ui_db_replica_path",
    "default_ui_replica_meta_path",
    "load_ui_replica_meta",
    "refresh_ui_db_replica_once",
    "run_ui_db_replica_loop",
]
