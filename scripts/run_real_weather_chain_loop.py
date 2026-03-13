#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "data" / "dev" / "real_weather_chain"
DEFAULT_INTERVAL_MINUTES = 10
DEFAULT_RECENT_WITHIN_DAYS = 14


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuously run the real weather ingress chain against open recent Gamma weather markets."
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--interval-minutes", type=int, default=DEFAULT_INTERVAL_MINUTES)
    parser.add_argument("--recent-within-days", type=int, default=DEFAULT_RECENT_WITHIN_DAYS)
    parser.add_argument("--with-agent", action="store_true", help="Backward-compatible no-op; the loop now enables agents by default.")
    parser.add_argument("--skip-agent", action="store_true", help="显式跳过 weather agent 链路，仅用于 debug/fallback。")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--force-rebuild-on-start", action="store_true")
    return parser.parse_args()


def build_smoke_command(args: argparse.Namespace, *, force_rebuild: bool) -> list[str]:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_real_weather_chain_smoke.py"),
        "--output-dir",
        str(Path(args.output_dir)),
        "--recent-within-days",
        str(int(args.recent_within_days)),
    ]
    if args.skip_agent:
        cmd.append("--skip-agent")
    if force_rebuild:
        cmd.append("--force-rebuild")
    return cmd


def write_status_report(
    report_path: Path,
    *,
    chain_status: str,
    note: str,
    recent_within_days: int,
    command: list[str],
) -> None:
    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "chain_status": chain_status,
        "report_scope": ["市场发现", "规则解析", "预测服务", "定价引擎", "机会发现"],
        "market_discovery": {
            "status": chain_status,
            "input_mode": "live_weather_market_auto_horizon",
            "market_id": None,
            "question": None,
            "discovered_count": 0,
            "note": note,
            "selected_horizon_days": int(recent_within_days),
        },
        "rule_parse": {"status": "skipped"},
        "forecast_service": {"status": "skipped"},
        "pricing_engine": {"status": "skipped"},
        "opportunity_discovery": {"status": "skipped"},
        "artifacts": {
            "report_path": str(report_path),
            "runner_command": command,
        },
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_cycle(args: argparse.Namespace, *, force_rebuild: bool) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "real_weather_chain_report.json"
    cmd = build_smoke_command(args, force_rebuild=force_rebuild)
    write_status_report(
        report_path,
        chain_status="initializing",
        note="市场链路正在生成首份或最新一轮报告，请稍候刷新页面。",
        recent_within_days=int(args.recent_within_days),
        command=cmd,
    )
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode == 0:
        if proc.stdout.strip():
            print(proc.stdout.strip())
        return 0
    error_text = (proc.stderr or proc.stdout).strip()
    if "transport_error:" in error_text:
        write_status_report(
            report_path,
            chain_status="transport_error",
            note=error_text,
            recent_within_days=int(args.recent_within_days),
            command=cmd,
        )
        print(error_text, file=sys.stderr)
        return 0
    if "no open recent weather markets found within" in error_text:
        write_status_report(
            report_path,
            chain_status="no_open_recent_markets",
            note=(
                f"当前没有命中筛选条件的开盘近期天气市场。系统未回落到历史冻结样本；"
                f"会继续按 {int(args.recent_within_days)} -> 30 -> 60 -> 90 天窗口重试。"
            ),
            recent_within_days=int(args.recent_within_days),
            command=cmd,
        )
        print(error_text)
        return 0
    write_status_report(
        report_path,
        chain_status="error",
        note=error_text or "real weather chain loop failed",
        recent_within_days=int(args.recent_within_days),
        command=cmd,
    )
    print(error_text or "real weather chain loop failed", file=sys.stderr)
    return proc.returncode or 1


def main() -> int:
    args = parse_args()
    if int(args.interval_minutes) <= 0:
        raise SystemExit("interval-minutes must be positive")
    if int(args.recent_within_days) <= 0:
        raise SystemExit("recent-within-days must be positive")

    first = True
    while True:
        rc = run_cycle(args, force_rebuild=bool(args.force_rebuild_on_start and first))
        first = False
        if args.once:
            return rc
        next_run = datetime.now(UTC) + timedelta(minutes=int(args.interval_minutes))
        print(
            f"[real-weather-loop] next retry at {next_run.isoformat()} "
            f"(interval={int(args.interval_minutes)}m, window={int(args.recent_within_days)}d)"
        )
        time.sleep(int(args.interval_minutes) * 60)


if __name__ == "__main__":
    raise SystemExit(main())
