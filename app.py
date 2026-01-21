import streamlit as st
import pandas as pd
import baostock as bs
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta
import requests

# --- 1. 配置 ---
st.set_page_config(page_title="游资核心追踪-原生版", layout="wide")

# --- 2. 核心业务引擎 ---

def get_all_mainboard_stocks():
    """环节一：从 Baostock 静默获取全市场主板股票代码"""
    # 获取证券股信息
    rs = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    # 转换为 DataFrame 方便过滤
    raw_df = pd.DataFrame(data_list, columns=rs.fields)
    
    # 核心过滤逻辑：
    # 1. code_name 不包含 "ST"
    # 2. code 以 "sh.60" 或 "sz.00" 开头 (主板)
    main_df = raw_df[
        (~raw_df['code_name'].str.contains("ST")) & 
        (raw_df['code'].str.startswith(('sh.60', 'sz.00')))
    ]
    
    # 返回 [[代码, 名称], ...] 格式
    return main_df[['code', 'code_name']].values.tolist()

def fetch_stock_analysis(bs_code, name):
    """环节二：筛查换手率 ≥ 3%"""
    try:
        # 获取最近20个交易日数据
        rs = bs.query_history_k_data_plus(bs_code,
            "date,open,high,low,close,volume,turnover",
            start_date=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d'),
            frequency="d", adjustflag="3")
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if len(data_list) < 8: return None
        
        df = pd.DataFrame(data_list, columns=["date","open","high","low","close","volume","turnover"])
        df[['open','high','low','close','volume','turnover']] = df[['open','high','low','close','volume','turnover']].apply(pd.to_numeric)
        
        latest_turnover = df.iloc[-1]['turnover']
        
        # 环节二硬指标：换手率 ≥ 3%
        if latest_turnover >= 3.0:
            return {"code": bs_code, "name": name, "df": df, "turnover": latest_turnover}
    except:
        return None
    return None

def check_positive_days(stock_obj):
    """环节三：连阳验证"""
    df = stock_obj['df']
    df['is_pos'] = df['close'] > df['open']
    pos_list = df['is_pos'].tolist()
    
    # 剔除 8 连阳风险
    if len(pos_list) >= 8 and all(pos_list[-8:]): return None

    for d, g_limit in [(7, 22.5), (6, 17.5), (5, 12.5)]:
        sub = df.tail(d)
        if (sub['close'] > sub['open']).all():
            gain = round(((sub.iloc[-1]['close'] - sub.iloc[0]['open']) / sub.iloc[0]['open']) * 100, 2)
            if gain <= g_limit:
                return {
                    "代码": stock_obj['code'].replace('sh.','').replace('sz.',''), 
                    "名称": stock_obj['name'], 
                    "换手率": f"{stock_obj['turnover']}%", 
                    "判定强度": f"{d}连阳", 
                    "区间涨幅": f"{gain}%", 
                    "最新价": round(df.iloc[-1]['close'], 2)
                }
    return None

# --- 3. 页面渲染 ---

st.title("🚀 游资核心追踪 (Baostock 原生全量扫描版)")

with st.sidebar:
    st.header("扫描设置")
    thread_num = st.slider("并发强度", 1, 15, 8)
    st.info("提示：此版本完全脱离 DeepSeek，直接从交易所接口拉取全市场主板数据进行穿透。")

if st.button("启动全市场扫描"):
    # 登录 Baostock
    bs.login()
    
    # 环节一：静默寻源
    with st.spinner("📦 环节一：正在拉取 A 股主板全名单..."):
        initial_list = get_all_mainboard_stocks()
    
    if initial_list:
        # 环节二：筛查换手率
        st.write(f"### 📍 环节二：活跃股筛选 (换手率 ≥ 3%, 待扫总量: {len(initial_list)})")
        passed_turnover = []
        progress_1 = st.progress(0.0)
        status_text = st.empty()
        
        with ThreadPoolExecutor(max_workers=thread_num) as executor:
            futures = {executor.submit(fetch_stock_analysis, s[0], s[1]): s for s in initial_list}
            for i, f in enumerate(as_completed(futures)):
                res = f.result()
                if res: passed_turnover.append(res)
                # 更新进度
                curr_progress = (i + 1) / len(initial_list)
                progress_1.progress(curr_progress)
                status_text.text(f"正在扫描: {i+1}/{len(initial_list)}")
        
        if passed_turnover:
            st.success(f"完成！在全市场发现 {len(passed_turnover)} 只活跃股 (换手率≥3%)")
            turn_df = pd.DataFrame([{"代码": x['code'], "名称": x['name'], "换手率": f"{x['turnover']}%"} for x in passed_turnover])
            st.dataframe(turn_df, use_container_width=True, height=250)

            # 环节三：连阳验证
            st.divider()
            st.write(f"### 🔥 环节三：5-7 连阳战法精选")
            final_results = []
            for obj in passed_turnover:
                res = check_positive_days(obj)
                if res:
                    final_results.append(res)
                    st.toast(f"✅ 捕获: {res['名称']}")

            if final_results:
                res_df = pd.DataFrame(final_results)
                res_df.insert(0, '序号', range(1, len(res_df) + 1))
                st.subheader("📋 最终决策分析报表")
                st.dataframe(res_df, use_container_width=True, hide_index=True)
                
                output = io.BytesIO()
                res_df.to_excel(output, index=False)
                st.download_button("📥 导出全场战报", output.getvalue(), f"全场扫描_{datetime.now().strftime('%m%d')}.xlsx")
            else:
                st.warning("环节三结束：全场活跃股中暂无符合 5-7 连阳逻辑的标的。")
        else:
            st.error("环节二结束：全场未发现换手率 ≥ 3% 的标的（可能今日未开盘或接口限制）。")
    
    # 退出登录
    bs.logout()

st.divider()
st.caption("Master Copy | 2026-01-21 | Baostock 原生全量驱动 | 剔除 ST/创业/科创")
