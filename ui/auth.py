"""Streamlit UI auth helpers."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import streamlit as st


ROOT = Path(__file__).resolve().parents[1]


def _maybe_load_project_dotenv() -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


_maybe_load_project_dotenv()


def _expected_username() -> str:
    return str(os.getenv("ASTERION_UI_USERNAME") or "").strip()


def _expected_password_hash() -> str:
    return str(os.getenv("ASTERION_UI_PASSWORD_HASH") or "").strip()


def ui_auth_config_status() -> str:
    if not _expected_username() or not _expected_password_hash():
        return "missing_credentials"
    return "configured"


def verify_ui_credentials(username: str, password: str) -> str:
    if ui_auth_config_status() != "configured":
        return "missing_credentials"
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    if username == _expected_username() and password_hash == _expected_password_hash():
        return "authenticated"
    return "invalid_credentials"


def enforce_ui_auth() -> str:
    config_status = ui_auth_config_status()
    if config_status == "missing_credentials":
        st.error("UI auth 未配置，默认拒绝访问。请设置 `ASTERION_UI_USERNAME` 和 `ASTERION_UI_PASSWORD_HASH`。")
        return "missing_credentials"

    if st.session_state.get("ui_auth_status") == "authenticated":
        return "authenticated"

    st.markdown("### Operator Login")
    st.caption("Phase 1: UI auth default-deny 已启用。未认证前不会渲染 operator console。")

    username = st.text_input("Username", key="ui_auth_username")
    password = st.text_input("Password", type="password", key="ui_auth_password")
    if st.button("Login", type="primary", key="ui_auth_submit"):
        result = verify_ui_credentials(username, password)
        st.session_state["ui_auth_status"] = result
        if result == "authenticated":
            st.session_state.pop("ui_auth_password", None)
            st.rerun()

    result = str(st.session_state.get("ui_auth_status") or "configured")
    if result == "invalid_credentials":
        st.error("认证失败：用户名或密码不正确。")
    else:
        st.info("请输入 operator 凭证后继续。")
    return result
