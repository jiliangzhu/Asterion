from __future__ import annotations

import dataclasses
import os
import time
from pathlib import Path
from typing import Any

from asterion_core.storage.utils import ensure_dir, safe_json_dumps


def _utc_date_from_ms(ms: int) -> str:
    return time.strftime("%Y-%m-%d", time.gmtime(ms / 1000.0))


def _utc_minute_from_ms(ms: int) -> str:
    return time.strftime("%H%M", time.gmtime(ms / 1000.0))


@dataclasses.dataclass
class BronzeJsonlRollingWriter:
    """Append-only JSONL writer that rolls files by UTC minute."""

    root_dir: str
    subdir: str
    part_prefix: str
    _cur_minute: str | None = None
    _cur_date: str | None = None
    _cur_tmp_path: Path | None = None
    _f: Any | None = None

    def _open_new(self, *, ts_ms: int) -> None:
        date = _utc_date_from_ms(ts_ms)
        minute = _utc_minute_from_ms(ts_ms)
        out_dir = Path(self.root_dir) / self.subdir / f"date={date}" / f"minute={minute}"
        ensure_dir(str(out_dir))
        suffix = f"{int(time.time() * 1000)}_{os.getpid()}"
        tmp_path = out_dir / f"{self.part_prefix}-{suffix}.jsonl.tmp"
        self._f = open(tmp_path, "a", encoding="utf-8", buffering=1)
        self._cur_date = date
        self._cur_minute = minute
        self._cur_tmp_path = tmp_path

    def _finalize_current(self) -> None:
        if self._f is None or self._cur_tmp_path is None:
            return
        try:
            self._f.flush()
        except Exception:
            pass
        try:
            self._f.close()
        except Exception:
            pass
        tmp_path = self._cur_tmp_path
        final_path = Path(str(tmp_path).removesuffix(".tmp"))
        tmp_path.rename(final_path)
        self._f = None
        self._cur_tmp_path = None

    def write(self, record: dict[str, Any], *, ts_ms: int | None = None) -> None:
        ts_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
        minute = _utc_minute_from_ms(ts_ms)
        if self._f is None:
            self._open_new(ts_ms=ts_ms)
        elif self._cur_minute != minute:
            self._finalize_current()
            self._open_new(ts_ms=ts_ms)
        assert self._f is not None
        self._f.write(safe_json_dumps(record))
        self._f.write("\n")

    def close(self) -> None:
        self._finalize_current()
