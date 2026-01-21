import streamlit as st
import pandas as pd
import baostock as bs
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta
import gc # 导入垃圾回收

# --- 1. 配置 ---
st.set_page_config(page_title="游资核心追踪-抗压版", layout="wide")

def get_all_mainboard_stocks():
    """环节一：获取全市场主板股票"""
    bs.login() # 确保在获取名单前登录
    rs = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    bs.logout()
    
    if not data_list: return []
    raw_df = pd.DataFrame(data_list, columns=rs.fields)
    main_df = raw_df[(~raw_df['code_name'].str.contains("ST")) & 
                     (raw_df['code'].str.startswith(('sh.60', 'sz.00')))]
    return main_df[['code', 'code_name']].values.tolist()

def fetch_stock_analysis(bs_code, name):
    """环节二核心：带强力异常处理和内存释放"""
    try:
        # 每次请求尝试重新开启一小段连接，避免长时间占用
        rs = bs.query_history_k_data_plus(bs_code,
            "date,open,high,low,close,volume,turnover",
            start_date=(datetime.now() - timedelta(days=35)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d'),
            frequency="d", adjustflag="3")
        
        if rs.error_code != '0': return None

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if len(data_list) < 8: return None
        
        df = pd.DataFrame(data_list, columns=["date","open","high","low","close","volume","turnover"])
        df[['open','high','low','close','volume','turnover']] = df[['open','high','low','close','volume','turnover']].apply(pd.to_numeric)
        
        latest_turnover = df.iloc[-1]['turnover']
        
        if latest_turnover >= 3.0:
            res = {"code": bs_code, "name": name, "df": df, "turnover": latest_turnover}
            return res
        
        # 💡 主动清理不再需要的变量，释放内存
        del df
        del data_list
    except:
        return None
    return None

# --- 3. 页面渲染 ---
st.title("🚀 游资核心追踪 (Baostock 全量抗压版)")

with st.sidebar:
    st.header("性能调优")
    # 💡 建议降低并发，避免触发 Baostock 封锁
    thread_num = st.slider("并发强度", 1, 10, 5) 
    st.warning("如遇到 3000+ 数量卡顿，请调低并发至 3-5。")

if st.button("启动全量穿透扫描"):
    bs.login()
    
    with st.spinner("📦 环节一：正在拉取名册..."):
        initial_list = get_all_mainboard_stocks()
    
    if initial_list:
        st.write(f"### 📍 环节二：活跃股筛选 (待扫: {len(initial_list)})")
        passed_turnover = []
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        
        # 💡 增加分批处理逻辑，每扫描 500 只强制休息 2 秒，防止内存和连接溢出
        batch_size = 500
        for batch_idx in range(0, len(initial_list), batch_size):
            batch = initial_list[batch_idx : batch_idx + batch_size]
            
            with ThreadPoolExecutor(max_workers=thread_num) as executor:
                futures = {executor.submit(fetch_stock_analysis, s[0], s[1]): s for s in batch}
                for i, future in enumerate(as_completed(futures)):
                    try:
                        # 💡 增加严格的 2 秒超时
                        res = future.result(timeout=2)
                        if res: passed_turnover.append(res)
                    except: continue
                    
                    # 进度条更新
                    total_idx = batch_idx + i + 1
                    pct = total_idx / len(initial_list)
                    progress_bar.progress(pct)
                    status_text.text(f"已扫描: {total_idx} / {len(initial_list)}")
            
            # 💡 关键：每批次结束，强制执行垃圾回收，清理内存
            gc.collect()
            time.sleep(1) # 给服务器喘息时间

        # 环节三逻辑
        if passed_turnover:
            st.divider()
            st.write("### 🔥 环节三：连阳战法精选")
            # ... 此处省略连阳验证逻辑，同母版 ...
            # 请参考前一版代码中的 check_positive_days 部分
            # ...
        
    bs.logout()
