from __future__ import annotations

from typing import Iterable

import pandas as pd
import streamlit as st


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value == ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    try:
        missing = pd.isna(value)
    except Exception:
        return False
    return bool(missing) if isinstance(missing, (bool,)) else False


def _safe_text(value: object) -> str:
    if _is_missing(value):
        return "N/A"
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (list, tuple, set)):
        return ", ".join(_safe_text(item) for item in value) or "N/A"
    return str(value)


def _tone_prefix(tone: str) -> str:
    return {
        "ok": "OK",
        "warn": "WARN",
        "err": "ERROR",
        "info": "INFO",
        "muted": "NOTE",
    }.get(tone, "INFO")


def render_page_intro(
    title: str,
    summary: str,
    *,
    kicker: str | None = None,
    badges: Iterable[tuple[str, str]] | None = None,
) -> None:
    left, right = st.columns([1.35, 1])
    with left:
        st.caption(kicker or "Operator Console")
        st.subheader(title)
    with right:
        if badges:
            badge_text = " | ".join(_safe_text(label) for label, _ in badges)
            st.caption(badge_text)
    st.write(summary)


def render_section_header(title: str, *, subtitle: str | None = None, eyebrow: str | None = None) -> None:
    if eyebrow:
        st.caption(eyebrow)
    st.markdown(f"### {title}")
    if subtitle:
        st.caption(subtitle)


def render_kpi_band(items: Iterable[dict[str, object]]) -> None:
    entries = list(items)
    if not entries:
        return
    columns = st.columns(len(entries))
    for col, item in zip(columns, entries, strict=True):
        with col:
            st.metric(
                str(item.get("label") or "Metric"),
                _safe_text(item.get("value")),
                delta=_safe_text(item.get("delta")) if not _is_missing(item.get("delta")) else None,
            )


def render_state_card(title: str, body: str, *, tone: str = "info", meta: str | None = None) -> None:
    with st.container(border=True):
        st.caption(f"{_tone_prefix(tone)} · {title}")
        st.write(body)
        if not _is_missing(meta):
            st.caption(meta)


def render_detail_key_value(rows: Iterable[tuple[str, object] | dict[str, object]]) -> None:
    normalized: list[tuple[str, object]] = []
    for row in rows:
        if isinstance(row, dict):
            label = row.get("字段") or row.get("label") or row.get("key") or ""
            value = row.get("值") if "值" in row else row.get("value")
        else:
            label, value = row
        normalized.append((str(label), value))

    with st.container(border=True):
        for label, value in normalized:
            left, right = st.columns([0.95, 2.05])
            with left:
                st.caption(_safe_text(label))
            with right:
                st.write(_safe_text(value))


def render_empty_state(title: str, body: str, *, tone: str = "muted") -> None:
    label = f"{_tone_prefix(tone)} · {title}"
    if tone == "err":
        st.error(f"{label}: {body}")
    elif tone == "warn":
        st.warning(f"{label}: {body}")
    elif tone == "ok":
        st.success(f"{label}: {body}")
    else:
        st.info(f"{label}: {body}")


def render_delivery_badge(status: object, *, origin: object | None = None) -> None:
    label = _safe_text("ok" if _is_missing(status) else status)
    suffix = f" · {_safe_text(origin)}" if not _is_missing(origin) else ""
    st.caption(f"Delivery: {label}{suffix}")


def render_reason_chip_row(reasons: Iterable[object], *, empty_label: str = "none") -> None:
    items = [str(item) for item in reasons if str(item).strip()]
    if not items:
        items = [empty_label]
    st.caption(" / ".join(items))
