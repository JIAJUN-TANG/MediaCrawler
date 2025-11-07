import streamlit as st
import pandas as pd
from utils.data_utils import get_table_structure, load_table_data, get_media


# 初始化会话状态（存储当前表格标识，用于切换时重置选择）
if "last_table_key" not in st.session_state:
    st.session_state.last_table_key = None


st.title("数据浏览")
st.write("通过下方选择框筛选数据，点击表格行可查看详情")

# 获取表结构并显示筛选框
table_info = get_table_structure()
if not table_info:
    st.warning("未检测到有效数据表，请检查数据库连接")
    st.stop()

# 平台和数据类型筛选
platforms = sorted(table_info.keys())
selected_platform = st.selectbox("选择平台", platforms, index=0, help="按表名前缀分组的平台")
data_types = table_info[selected_platform]
selected_data_type = st.selectbox("选择数据类型", data_types, index=0, help="按表名后缀区分的数据类型")

# 生成当前表唯一标识（用于切换表格时重置选择状态）
current_table_key = f"{selected_platform}_{selected_data_type}"

st.divider()

# 加载数据
with st.spinner(f"正在加载 {selected_platform} - {selected_data_type} 的数据..."):
    df = load_table_data(selected_platform, selected_data_type)

    # 切换表格后重置选择状态（避免显示旧表的选中数据）
    if current_table_key != st.session_state.last_table_key:
        st.session_state.last_table_key = current_table_key


if df is not None and not df.empty:
    st.subheader(f"{selected_platform} - {selected_data_type} 表详情")
    
    # 显示表格并启用单行选择，设置 on_select="rerun" 触发刷新
    event = st.dataframe(
        df,
        hide_index=True,
        selection_mode="single-row",  # 仅允许单行选择
        key=current_table_key,
        on_select="rerun"  # 选中行时重新运行脚本，更新选择状态
    )

    selection = event.selection
    selected_rows = selection.rows  # 获取选中的行索引列表
    st.caption(f"共 {len(df):,} 条数据")

    st.divider()
    st.subheader("数据详情")
    col1, col2 = st.columns(2)

    if selected_rows:
        selected_idx = selected_rows[0]
        selected_row_data = df.iloc[selected_idx].to_dict()
        id_fields = [
            (field, value) 
            for field, value in selected_row_data.items() 
            if field == "note_id" or field == "aweme_id" or field == "video_id"
        ]

        with col1:
            with st.container(border=True):
                for field, value in selected_row_data.items():
                    if pd.isna(value):
                        display_val = "（空值）"
                    elif isinstance(value, str) and len(value) > 600:
                        display_val = value[:600] + "..."
                        with st.expander("查看完整内容"):
                            st.text(value)
                    elif isinstance(value, (list, dict)):
                        display_val = str(value).replace(",", ",\n")  # 换行增强可读性
                    else:
                        display_val = value

                    st.markdown(f"**{field}**：\n{display_val}")
        
        with col2:
            if id_fields:
                media = get_media(selected_platform, id_fields[0][1])
                if media:
                    for _ in media:
                        if _.split(".")[1] == "jpg":
                            st.image(_)
                        else:
                            st.video(_)
                        st.divider()
    else:
        st.info("请点击查看详细信息")
else:
    st.warning("数据加载失败，请检查数据表是否为空")