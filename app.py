import streamlit as st
import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="单次涨停回调筛选-稳定版", layout="wide")

# 初始化 Baostock 登录 (全局只做一次)
def init_bs():
    if 'bs_login' not in st.session_state:
        lg = bs.login()
        if lg.error_code == '0':
            st.session_state['bs_login'] = True
            return True
        return False
    return True

def fetch_data(code, name, start_date, end_date):
    """线程执行体：只负责抓取数据和逻辑判断"""
    try:
        # 注意：Baostock query 必须在 login 状态下，但在 ThreadPool 中共享主进程连接
        rs = bs.query_history_k_data_plus(
            code, "date,code,close,pctChg",
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag="3"
        )
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if len(data_list) < 10: return None
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        df['pctChg'] = pd.to_numeric(df['pctChg'])
        
        # 核心逻辑：过去 14 天（含今天）
        recent = df.tail(14)
        # 涨停判定放宽至 9.7% 容错
        limit_up_mask = recent['pctChg'] >= 9.7
        
        if limit_up_mask.sum() == 1:
            last_idx = recent[limit_up_mask].index[0]
            days_since = (len(df) - 1) - last_idx
            return {
                "代码": code, "名称": name, 
                "现价": recent.iloc[-1]['close'], 
                "今日涨幅": f"{recent.iloc[-1]['pctChg']}%",
                "距涨停天数": days_since
            }
    except:
        return None
    return None

def main():
    st.title("📊 单次涨停回调筛选器 (稳定加速版)")
    
    if not init_bs():
        st.error("Baostock 登录失败，请检查网络。")
        return

    # 控制区
    col1, col2 = st.columns([1, 4])
    with col1:
        run_btn = st.button("🚀 开始筛选全市场")
    
    if run_btn:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")
        
        # 1. 获取清单
        with st.spinner("获取 A 股清单中..."):
            stock_rs = bs.query_all_stock(day=end_date)
            raw_list = []
            while (stock_rs.error_code == '0') & stock_rs.next():
                raw_list.append(stock_rs.get_row_data())
        
        if not raw_list:
            st.error("接口未返回股票列表，请尝试刷新页面重试。")
            return
            
        # 2. 预过滤 (ST/创业板/科创板)
        filtered_stocks = []
        for s in raw_list:
            code, name = s[0], s[1]
            if "ST" in name or "st" in name: continue
            if code.split('.')[1].startswith(('300', '688')): continue
            filtered_stocks.append((code, name))
            
        # 3. 多线程处理
        final_results = []
        progress_bar = st.progress(0)
        status = st.empty()
        
        total = len(filtered_stocks)
        # 线程数不宜过大，防止被封 IP
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_data, s[0], s[1], start_date, end_date): s for s in filtered_stocks}
            
            for i, future in enumerate(as_completed(futures)):
                res = future.result()
                if res:
                    final_results.append(res)
                
                if i % 50 == 0:
                    progress_bar.progress((i + 1) / total)
                    status.text(f"已扫描 {i+1}/{total} 只个股...")
        
        status.success(f"扫描完毕！共发现 {len(final_results)} 只符合条件的股票。")
        progress_bar.empty()

        # 4. 展示与导出
        if final_results:
            df = pd.DataFrame(final_results)
            df.index = range(1, len(df) + 1) # 序号从1开始
            st.dataframe(df, use_container_width=True)
            
            csv = df.to_csv(index=True).encode('utf-8-sig')
            st.download_button("📥 导出 CSV 结果", csv, "stock_results.csv", "text/csv")
        else:
            st.warning("满足『14天内仅1次涨停』条件的个股为 0，建议确认最近两周行情。")

if __name__ == "__main__":
    main()
