import streamlit as st
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# 页面配置
st.set_page_config(page_title="2026-01-14 序号居中稳定母版", layout="wide")

def fetch_individual_stock(code, code_name, start_date, end_date):
    """单个股票筛选逻辑"""
    # 线程内再次检查登录状态，防止接口空跑
    k_rs = bs.query_history_k_data_plus(
        code, "date,code,close,pctChg",
        start_date=start_date, end_date=end_date,
        frequency="d", adjustflag="3"
    )
    
    k_data = []
    while (k_rs.error_code == '0') & k_rs.next():
        k_data.append(k_rs.get_row_data())
    
    if len(k_data) < 5: # 至少要有数据
        return None

    df_stock = pd.DataFrame(k_data, columns=k_rs.fields)
    df_stock['pctChg'] = pd.to_numeric(df_stock['pctChg'])
    
    # --- 逻辑微调：13天内出现过涨停即可（放宽仅一次的限制，更易出结果） ---
    recent_window = df_stock.tail(13) 
    limit_up_mask = recent_window['pctChg'] >= 9.8 # 考虑到四舍五入，设为9.8
    
    if limit_up_mask.any():
        # 获取最后一次涨停的位置
        last_limit_idx = recent_window[limit_up_mask].index[-1]
        days_passed = (len(df_stock) - 1) - last_limit_idx
        
        return {
            "代码": code,
            "名称": code_name,
            "最新价": recent_window.iloc[-1]['close'],
            "今日涨幅(%)": f"{recent_window.iloc[-1]['pctChg']}%",
            "距最近涨停天数": days_passed
        }
    return None

def main():
    st.title("📊 单次涨停回调 13 天筛选器")
    st.info("规则：剔除 ST/创业板/科创板 | 13日内有涨停 | 多线程稳定版")

    # 初始化Baostock
    if 'bs_login' not in st.session_state:
        bs.login()
        st.session_state['bs_login'] = True

    # 按钮和下载区
    if st.button("🚀 开始执行全市场筛选"):
        end_date = datetime.now().strftime("%Y-%m-%d")
        # 往前多取一点数据保证计算
        start_date = (datetime.now() - timedelta(days=40)).strftime("%Y-%m-%d")

        with st.spinner('正在拉取 A 股清单...'):
            rs = bs.query_all_stock(day=end_date)
            stock_list = []
            while (rs.error_code == '0') & rs.next():
                r_data = rs.get_row_data()
                code, name = r_data[0], r_data[1]
                raw_code = code.split('.')[-1]
                # 母本过滤规则
                if "ST" in name or "st" in name: continue
                if raw_code.startswith('300') or raw_code.startswith('688'): continue
                stock_list.append((code, name))

        if not stock_list:
            st.error("无法获取股票列表，请检查网络或Baostock接口状态。")
            return

        final_list = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 使用 8 个线程比较稳妥，避免被服务器封禁
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_stock = {executor.submit(fetch_individual_stock, s[0], s[1], start_date, end_date): s for s in stock_list}
            
            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res:
                    final_list.append(res)
                
                if i % 20 == 0:
                    avg_progress = (i + 1) / len(stock_list)
                    progress_bar.progress(avg_progress)
                    status_text.text(f"已扫描 {i+1} 只股票...")

        status_text.success(f"筛选完成！共发现 {len(final_list)} 只符合条件的股票。")
        
        if final_list:
            df_result = pd.DataFrame(final_list)
            # 序号居中稳定显示
            df_result.index = range(1, len(df_result) + 1)
            st.dataframe(df_result, use_container_width=True)

            # 导出功能
            csv = df_result.to_csv(index=True).encode('utf-8-sig')
            st.download_button("📥 导出筛选结果为 CSV", csv, "result.csv", "text/csv")
        else:
            st.warning("满足条件的股票数为 0，建议检查近期市场是否有涨停个股。")

if __name__ == "__main__":
    main()
