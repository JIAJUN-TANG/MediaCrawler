import streamlit as st


st.set_page_config(
    page_title="适老机器人多源数据管理平台",
    page_icon="📊",
    layout="wide",
)

pg = st.navigation([
    st.Page("./pages/homepage.py", title="数据概况", icon="🏠"),
    st.Page("./pages/preview.py", title="数据浏览", icon="📰"),
])
pg.run()