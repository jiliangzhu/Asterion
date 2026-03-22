from __future__ import annotations

from html import escape
from textwrap import dedent
from typing import Iterable

import streamlit as st


def _safe_text(value: object) -> str:
    if value is None or value == "":
        return "N/A"
    if isinstance(value, float):
        return f"{value:.2f}".rstrip("0").rstrip(".")
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, (list, tuple, set)):
        return ", ".join(_safe_text(item) for item in value) or "N/A"
    return str(value)


def _tone_class(tone: str) -> str:
    allowed = {"muted", "info", "ok", "warn", "err"}
    return tone if tone in allowed else "muted"


def render_page_intro(
    title: str,
    summary: str,
    *,
    kicker: str | None = None,
    badges: Iterable[tuple[str, str]] | None = None,
) -> None:
    badge_html = ""
    if badges:
        badge_html = "".join(
            f'<span class="ui-inline-badge {_tone_class(tone)}">{escape(_safe_text(label))}</span>'
            for label, tone in badges
        )
    st.markdown(
        dedent(
            f"""
            <div class="ui-page-intro">
                <div class="ui-page-intro__top">
                    <div>
                        <div class="ui-page-intro__kicker">{escape(kicker or "Operator Console")}</div>
                        <h2 class="ui-page-intro__title">{escape(title)}</h2>
                    </div>
                    <div class="ui-page-intro__badges">{badge_html}</div>
                </div>
                <p class="ui-page-intro__summary">{escape(summary)}</p>
            </div>
            """
        ).strip(),
        unsafe_allow_html=True,
    )


def render_section_header(title: str, *, subtitle: str | None = None, eyebrow: str | None = None) -> None:
    subtitle_html = (
        f'<p class="ui-section-header__subtitle">{escape(subtitle)}</p>' if subtitle else ""
    )
    eyebrow_html = f'<div class="ui-section-header__eyebrow">{escape(eyebrow)}</div>' if eyebrow else ""
    st.markdown(
        dedent(
            f"""
            <div class="ui-section-header">
                {eyebrow_html}
                <h3 class="ui-section-header__title">{escape(title)}</h3>
                {subtitle_html}
            </div>
            """
        ).strip(),
        unsafe_allow_html=True,
    )


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
                delta=_safe_text(item.get("delta")) if item.get("delta") not in {None, ""} else None,
            )


def render_state_card(title: str, body: str, *, tone: str = "info", meta: str | None = None) -> None:
    meta_html = f'<div class="ui-state-card__meta">{escape(meta)}</div>' if meta else ""
    st.markdown(
        dedent(
            f"""
            <div class="ui-state-card {_tone_class(tone)}">
                <div class="ui-state-card__title">{escape(title)}</div>
                <div class="ui-state-card__body">{escape(body)}</div>
                {meta_html}
            </div>
            """
        ).strip(),
        unsafe_allow_html=True,
    )


def render_detail_key_value(rows: Iterable[tuple[str, object] | dict[str, object]]) -> None:
    normalized: list[tuple[str, object]] = []
    for row in rows:
        if isinstance(row, dict):
            label = row.get("字段") or row.get("label") or row.get("key") or ""
            value = row.get("值") if "值" in row else row.get("value")
        else:
            label, value = row
        normalized.append((str(label), value))

    html_rows = "".join(
        dedent(
            f"""
            <div class="ui-kv-row">
                <div class="ui-kv-row__label">{escape(_safe_text(label))}</div>
                <div class="ui-kv-row__value">{escape(_safe_text(value))}</div>
            </div>
            """
        ).strip()
        for label, value in normalized
    )
    st.markdown(f'<div class="ui-kv-grid">{html_rows}</div>', unsafe_allow_html=True)


def render_empty_state(title: str, body: str, *, tone: str = "muted") -> None:
    st.markdown(
        dedent(
            f"""
            <div class="ui-empty-state {_tone_class(tone)}">
                <div class="ui-empty-state__title">{escape(title)}</div>
                <div class="ui-empty-state__body">{escape(body)}</div>
            </div>
            """
        ).strip(),
        unsafe_allow_html=True,
    )


def render_delivery_badge(status: object, *, origin: object | None = None) -> None:
    label = _safe_text(status or "ok")
    tone = {
        "ok": "ok",
        "degraded_source": "warn",
        "stale": "warn",
        "read_error": "err",
        "missing": "err",
    }.get(str(status), "info")
    suffix = f" · {escape(_safe_text(origin))}" if origin not in {None, ""} else ""
    st.markdown(
        f'<div class="ui-delivery-line"><span class="ui-inline-badge {tone}">{escape(label)}</span>{suffix}</div>',
        unsafe_allow_html=True,
    )


def render_reason_chip_row(reasons: Iterable[object], *, empty_label: str = "none") -> None:
    items = [str(item) for item in reasons if str(item).strip()]
    if not items:
        items = [empty_label]
    html = "".join(f'<span class="ui-reason-chip">{escape(item)}</span>' for item in items)
    st.markdown(f'<div class="ui-reason-chip-row">{html}</div>', unsafe_allow_html=True)
