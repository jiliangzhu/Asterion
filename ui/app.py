from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.auth import enforce_ui_auth
from ui.data_access import load_boundary_sidebar_truth, load_operator_surface_status
from ui.pages import agents, execution, home, markets, system
from ui.runtime_env import load_ui_runtime_boundary_status


st.set_page_config(
    page_title="Asterion Ops Console",
    page_icon="A",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        :root {
            --bg: #f4f1ea;
            --panel: #fbf8f2;
            --ink: #172126;
            --muted: #6a756f;
            --ok: #1f7a54;
            --warn: #c7891b;
            --err: #b2432f;
            --accent: #0f5c73;
            --border: rgba(23, 33, 38, 0.10);
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(15, 92, 115, 0.12), transparent 28%),
                radial-gradient(circle at top right, rgba(199, 137, 27, 0.12), transparent 24%),
                var(--bg);
            color: var(--ink);
        }

        .block-container {
            padding-top: 1.4rem;
            padding-bottom: 3rem;
            max-width: 1440px;
        }

        section[data-testid="stSidebar"] {
            background: #163038;
            border-right: 1px solid rgba(255, 255, 255, 0.08);
        }

        section[data-testid="stSidebar"] * {
            color: #ecf1ed !important;
        }

        div[data-testid="stMetric"] {
            background: var(--panel);
            border: 1px solid var(--border);
            border-radius: 18px;
            padding: 1rem 1.1rem;
            min-height: 122px;
            box-shadow: 0 10px 30px rgba(23, 33, 38, 0.06);
        }

        .console-shell {
            background: linear-gradient(140deg, rgba(251, 248, 242, 0.95), rgba(248, 244, 236, 0.92));
            border: 1px solid var(--border);
            border-radius: 24px;
            padding: 1.35rem 1.5rem;
            box-shadow: 0 18px 40px rgba(23, 33, 38, 0.08);
            margin-bottom: 1rem;
        }

        .console-title {
            display: flex;
            justify-content: space-between;
            gap: 1rem;
            align-items: flex-start;
        }

        .console-kicker {
            color: var(--accent);
            font-size: 0.82rem;
            font-weight: 700;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
        }

        .console-heading {
            margin: 0;
            color: var(--ink);
            font-size: 2.4rem;
            line-height: 1.06;
        }

        .console-subcopy {
            color: var(--muted);
            max-width: 64rem;
            line-height: 1.7;
            margin-top: 0.8rem;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            border-radius: 999px;
            font-size: 0.83rem;
            font-weight: 700;
            padding: 0.4rem 0.75rem;
            border: 1px solid transparent;
            white-space: nowrap;
        }

        .status-badge.ok {
            color: #15573c;
            background: rgba(31, 122, 84, 0.12);
            border-color: rgba(31, 122, 84, 0.18);
        }

        .status-badge.warn {
            color: #7a5406;
            background: rgba(199, 137, 27, 0.14);
            border-color: rgba(199, 137, 27, 0.22);
        }

        .status-badge.err {
            color: #7f3022;
            background: rgba(178, 67, 47, 0.14);
            border-color: rgba(178, 67, 47, 0.22);
        }

        .status-badge.info {
            color: #0f5c73;
            background: rgba(15, 92, 115, 0.12);
            border-color: rgba(15, 92, 115, 0.20);
        }

        .section-title {
            color: var(--ink);
            font-size: 1.12rem;
            font-weight: 700;
            margin: 0.6rem 0 0.9rem 0;
        }

        .inline-note {
            color: var(--muted);
            font-size: 0.92rem;
        }

        div[data-testid="stDataFrame"] {
            border: 1px solid var(--border);
            border-radius: 18px;
            overflow: hidden;
            background: rgba(251, 248, 242, 0.82);
        }

        div[data-testid="stAlert"] {
            border-radius: 16px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def _render_status_badge(label: str, tone: str = "info") -> str:
    return f'<span class="status-badge {tone}">{label}</span>'


def _render_shell_header() -> None:
    st.markdown(
        f"""
        <div class="console-shell">
            <div class="console-title">
                <div>
                    <div class="console-kicker">Asterion Ops Console</div>
                    <h1 class="console-heading">Operator Console for Constrained Execution</h1>
                </div>
                <div>
                    {_render_status_badge("v2.0 implementation active", "ok")}
                    {_render_status_badge("P4/remediation accepted", "warn")}
                </div>
            </div>
            <div class="console-subcopy">
                当前 UI 的定位是 operator console + constrained execution infra：聚焦机会优先的 weather markets、execution science、
                live-prereq wallet / execution、readiness evidence 与 controlled-live boundary。这里不是 unattended live，也不是 unrestricted live；
                当前默认口径是 P4 accepted; post-P4 remediation accepted; v2.0 implementation active。
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_global_surface_banner() -> None:
    surface_status = load_operator_surface_status()
    overall = surface_status["overall"]
    status = overall["status"]
    detail = overall["detail"]
    surface = overall["surface"]
    source = overall["source"]

    if status == "ok":
        return

    message = f"{overall['label']} · surface={surface} · source={source}"
    if detail:
        message = f"{message}\n\n{detail}"

    if status == "read_error":
        st.error(message)
    elif status == "degraded_source":
        st.warning(message)
    else:
        st.info(message)


PAGES = {
    "Home": ("决策首页", home.show),
    "Markets": ("机会终端", markets.show),
    "Execution": ("Execution Science", execution.show),
    "Agents": ("Exception Review", agents.show),
    "System": ("Readiness Evidence", system.show),
}

auth_status = enforce_ui_auth()
if auth_status != "authenticated":
    st.stop()

ui_boundary_status = load_ui_runtime_boundary_status()
if ui_boundary_status.status != "ok":
    st.error(
        "UI runtime boundary blocked. "
        f"bind_scope={ui_boundary_status.bind_scope} "
        f"reason_codes={', '.join(ui_boundary_status.reason_codes) or 'unknown'} "
        f"banned_env_categories={', '.join(ui_boundary_status.banned_env_categories) or 'none'}"
    )
    st.stop()


_render_shell_header()
_render_global_surface_banner()

st.sidebar.markdown("## Navigation")
page_key = st.sidebar.radio(
    "选择控制台页面",
    list(PAGES.keys()),
    index=0,
    format_func=lambda key: f"{key} · {PAGES[key][0]}",
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 当前边界")
sidebar_truth = load_boundary_sidebar_truth()
for item in sidebar_truth["capability_boundary"]:
    st.sidebar.markdown(f"- `{item}`")
for item in sidebar_truth["live_negations"]:
    st.sidebar.markdown(f"- `{item}`")

st.sidebar.markdown("---")
st.sidebar.caption(
    " | ".join(
        [
            "Asterion v1.2",
            sidebar_truth["current_phase_status"],
            f"truth-source={sidebar_truth['truth_source_doc']}",
        ]
    )
)

PAGES[page_key][1]()
