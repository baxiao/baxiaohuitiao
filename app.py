import streamlit as st
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# 页面配置
st.set_page_config(page_title="2026-01-14 序号居中稳定母版", layout="wide")

def fetch_individual_stock(code, code_name, start_date, end_date):
    """
    单个股票的筛选逻辑，用于多线程调用
    """
    # 每个线程需要独立登录或确保bs连接可用（Baostock在高并发下可能不稳定，这里采用逻辑分块）
    k_rs = bs.query_history_k_data_plus(
        code, "date,code,close,pctChg",
        start_date=start_date, end_date=end_date,
        frequency="d", adjustflag="3"
    )
    
    k_data = []
    while (k_rs.error_code == '0') & k_rs.next():
        k_data.append(k_rs.get_row_data())
    
    if len(k_data) < 14:
        return None

    df_stock = pd.DataFrame(k_data, columns=k_rs.fields)
    df_stock['pctChg'] = pd.to_numeric(df_stock['pctChg'])
    
    # 核心逻辑：14天内（1天涨停+13天回调）有且仅有一次涨停
    recent_window = df_stock.tail(14)
    limit_up_mask = recent_window['pctChg'] >= 9.9
    if limit_up_mask.sum() == 1:
        limit_up_idx = recent_window[limit_up_mask].index[0]
        days_passed = (len(df_stock) - 1) - limit_up_idx
        return {
            "代码": code,
            "名称": code_name,
            "现价": recent_window.iloc[-1]['close'],
            "今日涨幅(%)": recent_window.iloc[-1]['pctChg'],
            "距涨停已过天数": days_passed
        }
    return None

def main():
    st.title("📊 单次涨停回调 13 天筛选器")
    st.info("规则：剔除 ST/创业板/科创板 | 13日内仅一次涨停 | 多线程加速版")

    # 初始化Baostock
    if 'bs_login' not in st.session_state:
        bs.login()
        st.session_state['bs_login'] = True

    # 1. 设置按钮区域
    col1, col2 = st.columns([1, 5])
    with col1:
        start_button = st.button("🚀 开始筛选")
    
    if start_button:
        # 获取日期
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")

        # 2. 获取股票列表并初步过滤
        with st.spinner('正在初始化 A 股列表...'):
            rs = bs.query_all_stock(day=end_date)
            stock_list = []
            while (rs.error_code == '0') & rs.next():
                r_data = rs.get_row_data()
                code, name = r_data[0], r_data[1]
                # 执行母本剔除规则
                raw_code = code.split('.')[-1]
                if "ST" in name or "st" in name: continue
                if raw_code.startswith('300') or raw_code.startswith('688'): continue
                stock_list.append((code, name))

        # 3. 多线程执行筛选
        final_list = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total = len(stock_list)
        # 建议开启 10-20 个线程，Baostock 接口有频率限制，不宜过高
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_stock = {executor.submit(fetch_individual_stock, s[0], s[1], start_date, end_date): s for s in stock_list}
            
            for i, future in enumerate(as_completed(future_to_stock)):
                result = future.result()
                if result:
                    final_list.append(result)
                
                # 更新进度条
                if i % 10 == 0 or i == total - 1:
                    avg_progress = (i + 1) / total
                    progress_bar.progress(avg_progress)
                    status_text.text(f"正在扫描第 {i+1}/{total} 只股票: {future_to_stock[future][1]}")

        status_text.success(f"筛选完成！共发现 {len(final_list)} 只符合条件的股票。")
        progress_bar.empty()

        # 4. 显示结果与导出
        if final_list:
            df_result = pd.DataFrame(final_list)
            # 序号居中处理：重置索引并从1开始
            df_result.index = range(1, len(df_result) + 1)
            
            st.dataframe(df_result, use_container_width=True)

            # 导出功能
            csv = df_result.to_csv(index=True).encode('utf-8-sig') # utf-8-sig 防止中文乱码
            st.download_button(
                label="📥 导出筛选结果为 CSV",
                data=csv,
                file_name=f'涨停回调筛选_{end_date}.csv',
                mime='text/csv',
            )
        else:
            st.warning("当前市场未发现符合条件的股票。")

if __name__ == "__main__":
    main()
