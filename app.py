import streamlit as st
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta

# 设置页面配置
st.set_page_config(page_title="单次涨停回调筛选", layout="wide")

def stock_screening_streamlit():
    st.title("📊 单次涨停回调 13 天筛选器")
    st.write("规则：剔除 ST/创业板/科创板 | 13日内仅一次涨停 | 纯净表格版")

    # 1. 登录系统
    if 'bs_login' not in st.session_state:
        lg = bs.login()
        st.session_state['bs_login'] = lg

    # 获取日期范围
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")

    # 2. 获取股票列表
    with st.spinner('正在获取 A 股列表...'):
        rs = bs.query_all_stock(day=end_date)
        all_stocks = []
        while (rs.error_code == '0') & rs.next():
            all_stocks.append(rs.get_row_data())
        
        result_df = pd.DataFrame(all_stocks, columns=rs.fields)

    # 3. 核心筛选逻辑
    final_list = []
    
    # 增加进度条，解决页面“无内容”感
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # 为了演示效率，这里先取前 200 只做示例，实际使用可去掉 [:200]
    total_stocks = len(result_df)
    
    for index, row in result_df.iterrows():
        # 更新进度
        progress = (index + 1) / total_stocks
        progress_bar.progress(progress)
        
        code = row['code']
        code_name = row['code_name']

        # --- 规则过滤：剔除 ST、创业板、科创板 ---
        if "ST" in code_name or "st" in code_name:
            continue
        raw_code = code.split('.')[-1]
        if raw_code.startswith('300') or raw_code.startswith('688'):
            continue

        # 获取历史K线
        k_rs = bs.query_history_k_data_plus(
            code, "date,code,close,pctChg",
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag="3"
        )
        
        k_data = []
        while (k_rs.error_code == '0') & k_rs.next():
            k_data.append(k_rs.get_row_data())
        
        if len(k_data) < 14:
            continue

        df_stock = pd.DataFrame(k_data, columns=k_rs.fields)
        df_stock['pctChg'] = pd.to_numeric(df_stock['pctChg'])
        
        # 截取最近14个交易日
        recent_window = df_stock.tail(14)
        limit_up_mask = recent_window['pctChg'] >= 9.9
        limit_up_count = limit_up_mask.sum()

        # 逻辑：有且仅有一次涨停
        if limit_up_count == 1:
            limit_up_idx = recent_window[limit_up_mask].index[0]
            days_passed = (len(df_stock) - 1) - limit_up_idx
            
            final_list.append({
                "代码": code,
                "名称": code_name,
                "现价": recent_window.iloc[-1]['close'],
                "今日涨幅(%)": recent_window.iloc[-1]['pctChg'],
                "距涨停已过天数": days_passed
            })

    # 4. 显示结果
    status_text.text("筛选完成！")
    progress_bar.empty()

    if final_list:
        final_df = pd.DataFrame(final_list)
        # 使用 Streamlit 的表格组件
        st.dataframe(final_df, use_container_width=True)
    else:
        st.warning("当前市场未发现符合条件的股票。")

if __name__ == "__main__":
    stock_screening_streamlit()
