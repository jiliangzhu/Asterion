"""Streamlit UI 认证"""
import streamlit as st
import hashlib
import os


def check_password() -> bool:
    """简单的密码认证"""
    def password_entered():
        username = st.session_state["username"]
        password = st.session_state["password"]

        expected_username = os.getenv("ASTERION_UI_USERNAME", "admin")
        expected_password_hash = os.getenv(
            "ASTERION_UI_PASSWORD_HASH",
            hashlib.sha256("changeme".encode()).hexdigest()
        )

        password_hash = hashlib.sha256(password.encode()).hexdigest()

        if username == expected_username and password_hash == expected_password_hash:
            st.session_state["authenticated"] = True
            del st.session_state["password"]
        else:
            st.session_state["authenticated"] = False

    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        st.text_input("Username", key="username")
        st.text_input("Password", type="password", key="password")
        st.button("Login", on_click=password_entered)
        return False
    else:
        return True
