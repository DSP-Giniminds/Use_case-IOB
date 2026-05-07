import streamlit as st

def global_session_guard():
    if "authenticated" not in st.session_state:
        st.error("Unauthorized access.")
        st.stop()

