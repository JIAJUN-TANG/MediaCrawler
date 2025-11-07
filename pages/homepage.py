import streamlit as st
from utils.data_utils import get_statistics
import pandas as pd


st.title("数据概况")

with st.spinner("加载数据中...", show_time=True):
    stats = get_statistics()

col1, col2 = st.columns(2)

with col1:
    st.metric(
    label="数据量",
    value=f"{stats['total_records']:,}" if stats else "无详情",
)
    
with col2:
    st.metric(
    label="更新日期",
    value=f"{stats['latest_crawled_date']}" if stats else "无详情",
)

st.divider()

st.subheader("各平台数据量统计")
if stats.get("table_counts"):
        chart_data = pd.DataFrame(
            list(stats["table_counts"].items()),
            columns=["平台", "数据量"]  # 定义列名，用于图表显示
        )
        chart_data = chart_data.sort_values(by="数据量", ascending=True)
        
        # 显示柱状图
        st.bar_chart(
            chart_data,
            x="平台",  # x轴为平台名
            y="数据量",  # y轴为记录数
            width="stretch"  # 自适应容器宽度
        )
else:
    st.warning("暂无统计数据")