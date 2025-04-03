import streamlit as st

st.set_page_config(page_title="Data Explorer", layout="wide")

st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["EDA", "Outbound Sizing", "Attribution Model"])

if page == "EDA":
    from eda import run_eda
    run_eda()
elif page == "Outbound Sizing":
    from outbound_sizing import run_outbound_sizing
    run_outbound_sizing()
elif page == "Attribution Model":
    from attribution import run_attribution
    run_attribution()