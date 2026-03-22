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
from ui.runtime_env import hydrate_ui_runtime_env, load_ui_runtime_boundary_status


st.set_page_config(
    page_title="Asterion Ops Console",
    page_icon="A",
    layout="wide",
    initial_sidebar_state="expanded",
)

hydrate_ui_runtime_env()

st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

        :root {
            --font-sans: "Geist", "SF Pro Display", "Segoe UI", sans-serif;
            --font-mono: "IBM Plex Mono", "SFMono-Regular", "Menlo", monospace;
            --bg: #f3efe7;
            --panel: rgba(251, 247, 239, 0.86);
            --panel-strong: #fbf8f2;
            --panel-soft: rgba(244, 238, 227, 0.78);
            --ink: #172126;
            --muted: #66716d;
            --ok: #2f6b54;
            --warn: #9b6f28;
            --err: #9a4837;
            --accent: #1d5d63;
            --accent-strong: #173f46;
            --border: rgba(23, 33, 38, 0.09);
            --shadow: 0 24px 60px rgba(24, 38, 41, 0.09);
            --radius-lg: 28px;
            --radius-md: 20px;
            --radius-sm: 14px;
        }

        .stApp {
            background:
                radial-gradient(circle at top left, rgba(29, 93, 99, 0.12), transparent 26%),
                radial-gradient(circle at 84% 8%, rgba(155, 111, 40, 0.10), transparent 24%),
                linear-gradient(180deg, rgba(255,255,255,0.22), rgba(255,255,255,0)),
                var(--bg);
            color: var(--ink);
            font-family: var(--font-sans);
        }

        .stApp, .stApp p, .stApp li, .stApp label {
            font-family: var(--font-sans);
        }

        .stApp [data-testid="stMetricValue"],
        .stApp code,
        .stApp pre,
        .stApp .stCodeBlock {
            font-family: var(--font-mono);
            font-variant-numeric: tabular-nums;
        }

        .block-container {
            padding-top: 1.25rem;
            padding-bottom: 3.4rem;
            max-width: 1440px;
        }

        section[data-testid="stSidebar"] {
            background:
                linear-gradient(180deg, rgba(9, 32, 37, 0.94), rgba(20, 48, 56, 0.96)),
                #153038;
            border-right: 1px solid rgba(255, 255, 255, 0.08);
        }

        section[data-testid="stSidebar"] * {
            color: #ecf1ed !important;
        }

        section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
            line-height: 1.6;
        }

        section[data-testid="stSidebar"] div[role="radiogroup"] > label {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.06);
            border-radius: 16px;
            padding: 0.5rem 0.7rem;
            margin-bottom: 0.45rem;
            transition: transform 220ms ease, border-color 220ms ease, background 220ms ease;
        }

        section[data-testid="stSidebar"] div[role="radiogroup"] > label:hover {
            transform: translateX(2px);
            border-color: rgba(113, 181, 185, 0.28);
            background: rgba(255, 255, 255, 0.06);
        }

        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(255, 252, 247, 0.95), rgba(248, 244, 236, 0.92));
            border: 1px solid var(--border);
            border-radius: 22px;
            padding: 1.05rem 1.15rem;
            min-height: 124px;
            box-shadow: 0 18px 44px rgba(23, 33, 38, 0.07);
        }

        div[data-testid="stMetric"] label {
            letter-spacing: 0.04em;
        }

        div[data-testid="stMetricValue"] {
            font-weight: 700;
            letter-spacing: -0.03em;
        }

        .console-shell {
            background:
                linear-gradient(140deg, rgba(251, 248, 242, 0.97), rgba(247, 242, 231, 0.92)),
                rgba(255, 255, 255, 0.5);
            border: 1px solid var(--border);
            border-radius: var(--radius-lg);
            padding: 1.5rem 1.6rem 1.35rem 1.6rem;
            box-shadow: var(--shadow);
            margin-bottom: 1.1rem;
            overflow: hidden;
        }

        .console-title {
            display: flex;
            justify-content: space-between;
            gap: 1rem;
            align-items: flex-start;
        }

        .console-kicker {
            color: var(--accent);
            font-size: 0.76rem;
            font-weight: 700;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
        }

        .console-heading {
            margin: 0;
            color: var(--ink);
            font-size: clamp(2.4rem, 5vw, 3.25rem);
            line-height: 0.98;
            letter-spacing: -0.045em;
        }

        .console-subcopy {
            color: var(--muted);
            max-width: 67ch;
            line-height: 1.72;
            margin-top: 0.9rem;
        }

        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            border-radius: 999px;
            font-size: 0.8rem;
            font-weight: 600;
            padding: 0.45rem 0.78rem;
            border: 1px solid transparent;
            white-space: nowrap;
            margin-left: 0.35rem;
        }

        .status-badge.ok {
            color: #1f5e49;
            background: rgba(47, 107, 84, 0.12);
            border-color: rgba(47, 107, 84, 0.18);
        }

        .status-badge.warn {
            color: #7b5924;
            background: rgba(155, 111, 40, 0.12);
            border-color: rgba(155, 111, 40, 0.2);
        }

        .status-badge.err {
            color: #7f3c30;
            background: rgba(154, 72, 55, 0.12);
            border-color: rgba(154, 72, 55, 0.2);
        }

        .status-badge.info {
            color: #1d5d63;
            background: rgba(29, 93, 99, 0.12);
            border-color: rgba(29, 93, 99, 0.18);
        }

        .ui-page-intro,
        .ui-state-card,
        .ui-empty-state,
        .ui-kv-grid {
            background: rgba(251, 248, 242, 0.72);
            border: 1px solid var(--border);
            box-shadow: 0 14px 36px rgba(23, 33, 38, 0.05);
        }

        .ui-page-intro {
            border-radius: var(--radius-md);
            padding: 1.15rem 1.2rem 1rem 1.2rem;
            margin-bottom: 1rem;
        }

        .ui-page-intro__top {
            display: flex;
            justify-content: space-between;
            gap: 1rem;
            align-items: flex-start;
        }

        .ui-page-intro__kicker,
        .ui-section-header__eyebrow {
            color: var(--accent);
            font-size: 0.74rem;
            font-weight: 700;
            letter-spacing: 0.16em;
            text-transform: uppercase;
            margin-bottom: 0.35rem;
        }

        .ui-page-intro__title {
            margin: 0;
            font-size: 1.8rem;
            line-height: 1.02;
            letter-spacing: -0.04em;
        }

        .ui-page-intro__summary {
            margin: 0.8rem 0 0 0;
            color: var(--muted);
            max-width: 70ch;
            line-height: 1.68;
        }

        .ui-page-intro__badges {
            display: flex;
            flex-wrap: wrap;
            justify-content: flex-end;
            gap: 0.45rem;
        }

        .ui-inline-badge {
            display: inline-flex;
            align-items: center;
            border-radius: 999px;
            padding: 0.38rem 0.7rem;
            font-size: 0.77rem;
            font-weight: 600;
            border: 1px solid transparent;
            line-height: 1;
        }

        .ui-inline-badge.ok {
            color: #1f5e49;
            background: rgba(47, 107, 84, 0.12);
            border-color: rgba(47, 107, 84, 0.18);
        }

        .ui-inline-badge.warn {
            color: #7b5924;
            background: rgba(155, 111, 40, 0.12);
            border-color: rgba(155, 111, 40, 0.2);
        }

        .ui-inline-badge.err {
            color: #7f3c30;
            background: rgba(154, 72, 55, 0.12);
            border-color: rgba(154, 72, 55, 0.2);
        }

        .ui-inline-badge.info,
        .ui-inline-badge.muted {
            color: #1d5d63;
            background: rgba(29, 93, 99, 0.1);
            border-color: rgba(29, 93, 99, 0.14);
        }

        .ui-section-header {
            margin: 0.4rem 0 0.85rem 0;
        }

        .ui-section-header__title {
            margin: 0;
            color: var(--ink);
            font-size: 1.1rem;
            font-weight: 700;
            letter-spacing: -0.02em;
        }

        .ui-section-header__subtitle {
            margin: 0.35rem 0 0 0;
            color: var(--muted);
            font-size: 0.95rem;
            line-height: 1.62;
            max-width: 72ch;
        }

        .ui-state-card,
        .ui-empty-state {
            border-radius: var(--radius-md);
            padding: 0.95rem 1rem;
            margin-bottom: 0.85rem;
        }

        .ui-state-card__title,
        .ui-empty-state__title {
            font-size: 0.82rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.1em;
            margin-bottom: 0.42rem;
        }

        .ui-state-card__body,
        .ui-empty-state__body {
            line-height: 1.65;
            color: var(--ink);
        }

        .ui-state-card__meta {
            margin-top: 0.5rem;
            color: var(--muted);
            font-size: 0.9rem;
        }

        .ui-state-card.ok .ui-state-card__title,
        .ui-empty-state.ok .ui-empty-state__title {
            color: var(--ok);
        }

        .ui-state-card.warn .ui-state-card__title,
        .ui-empty-state.warn .ui-empty-state__title {
            color: var(--warn);
        }

        .ui-state-card.err .ui-state-card__title,
        .ui-empty-state.err .ui-empty-state__title {
            color: var(--err);
        }

        .ui-state-card.info .ui-state-card__title,
        .ui-empty-state.info .ui-empty-state__title,
        .ui-state-card.muted .ui-state-card__title,
        .ui-empty-state.muted .ui-empty-state__title {
            color: var(--accent);
        }

        .ui-kv-grid {
            border-radius: var(--radius-md);
            overflow: hidden;
            margin-bottom: 0.9rem;
        }

        .ui-kv-row {
            display: grid;
            grid-template-columns: minmax(120px, 200px) 1fr;
            gap: 1rem;
            padding: 0.78rem 1rem;
            border-bottom: 1px solid rgba(23, 33, 38, 0.06);
        }

        .ui-kv-row:last-child {
            border-bottom: none;
        }

        .ui-kv-row__label {
            color: var(--muted);
            font-size: 0.84rem;
            letter-spacing: 0.06em;
            text-transform: uppercase;
        }

        .ui-kv-row__value {
            color: var(--ink);
            line-height: 1.55;
            font-variant-numeric: tabular-nums;
        }

        .ui-reason-chip-row,
        .ui-delivery-line {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
            align-items: center;
            margin: 0.2rem 0 0.7rem 0;
            color: var(--muted);
            font-size: 0.9rem;
        }

        .ui-reason-chip {
            display: inline-flex;
            align-items: center;
            padding: 0.28rem 0.56rem;
            border-radius: 999px;
            background: rgba(29, 93, 99, 0.08);
            border: 1px solid rgba(29, 93, 99, 0.12);
            color: var(--accent-strong);
            font-size: 0.8rem;
            font-family: var(--font-mono);
        }

        .inline-note {
            color: var(--muted);
            font-size: 0.92rem;
        }

        div[data-testid="stDataFrame"] {
            border: 1px solid var(--border);
            border-radius: 20px;
            overflow: hidden;
            background: rgba(251, 248, 242, 0.88);
            box-shadow: 0 14px 32px rgba(23, 33, 38, 0.04);
        }

        div[data-testid="stAlert"] {
            border-radius: 16px;
        }

        button[kind],
        div[data-baseweb="select"] > div,
        div[data-testid="stExpander"],
        button[data-baseweb="tab"] {
            transition: transform 220ms ease, box-shadow 220ms ease, border-color 220ms ease, background 220ms ease;
        }

        button[kind] {
            border-radius: 14px;
            min-height: 2.85rem;
            font-weight: 600;
        }

        button[kind]:hover {
            transform: translateY(-1px);
        }

        button[kind]:active {
            transform: translateY(1px) scale(0.99);
        }

        button[data-baseweb="tab"] {
            border-radius: 999px !important;
            font-weight: 600;
        }

        div[data-testid="stExpander"] {
            border: 1px solid var(--border);
            border-radius: 18px;
            background: rgba(251, 248, 242, 0.72);
        }

        .stApp a, .stApp a:visited {
            color: var(--accent-strong);
        }

        .stApp [data-testid="stMarkdownContainer"] h3,
        .stApp [data-testid="stMarkdownContainer"] h4 {
            letter-spacing: -0.02em;
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
                    {_render_status_badge("research console", "info")}
                </div>
            </div>
            <div class="console-subcopy">
                当前 UI 的定位是 operator console + constrained execution infra：聚焦机会优先的 weather markets、execution science、
                live-prereq wallet / execution、readiness evidence 与 controlled-live boundary。这里不是 unattended live，也不是 unrestricted live；
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
    
PAGES[page_key][1]()
