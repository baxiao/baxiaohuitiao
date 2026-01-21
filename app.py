import streamlit as st
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta, timezone
import requests
import json

# --- 1. 配置与安全 (严格遵循母版) ---
st.set_page_config(page_title="游资核心追踪-纯腾讯版", layout="wide")

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        pwd = st.text_input("请输入访问令牌", type="password")
        if st.button("验证登录"):
            target_pwd = st.secrets.get("STOCK_SCAN_PWD")
            if pwd == target_pwd:
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("令牌错误")
        return False
    return True

# --- 2. 腾讯原生 API 工具箱 (无 Akshare 依赖) ---

def get_tencent_pool():
    """获取全市场名单 (腾讯接口保底)"""
    url = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&fltt=2&inv=2&fid=f3&fs=m:0+t:6,m:1+t:2&fields=f12,f14,f2,f3,f8"
    try:
        r = requests.get(url, timeout=5)
        data = r.json()['data']['diff']
        df = pd.DataFrame(data.values())
        df.columns = ['代码', '名称', '最新价', '涨跌幅', '换手率']
        return df
    except:
        return pd.DataFrame()

def fetch_tencent_kline(code):
    """获取腾讯复权K线"""
    symbol = f"sh{code}" if code.startswith("60") else f"sz{code}"
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,40,qfq"
    try:
        r = requests.get(url, timeout=5)
        res = r.json()
        k_data = res['data'][symbol]['qfqday'] if 'qfqday' in res['data'][symbol] else res['data'][symbol]['day']
        df = pd.DataFrame(k_data, columns=['date', 'open', 'close', 'high', 'low', 'volume'])
        df[['open', 'close']] = df[['open', 'close']].astype(float)
        return df
    except:
        return None

def is_limit_up(close, pre_close):
    if pd.isna(pre_close) or pre_close == 0: return False
    return close >= round(pre_close * 1.10 - 0.01, 2)

def process_single_stock(code, name, price, turnover, sector):
    try:
        df = fetch_tencent_kline(code)
        if df is None or len(df) < 25: return None
        
        df['pre_close'] = df['close'].shift(1)
        df['is_zt'] = df.apply(lambda x: is_limit_up(x['close'], x['pre_close']), axis=1)
        
        # --- 严格13日判定逻辑 ---
        target_idx = len(df) - 14 # 锁定13个交易日前
        if target_idx < 0: return None
        
        # 13天前是涨停阳线
        if df.loc[target_idx, 'is_zt'] and df.loc[target_idx, 'close'] > df.loc[target_idx, 'open']:
            after_zt = df.loc[target_idx + 1 :, 'is_zt'].sum()
            
            res_type = ""
            if after_zt > 0:
                if df.loc[target_idx + 1 : target_idx + 10, 'is_zt'].any():
                    res_type = "10天双涨停-仅回调13天"
            elif after_zt == 0:
                res_type = "单次涨停-仅回调13天"
            
            # 今日未涨停且满足条件
            if res_type and not df.iloc[-1]['is_zt']:
                return {
                    "代码": code, "名称": name, "当前价格": price, "换手率": f"{turnover}%",
                    "判定强度": res_type, "决策": "13日临界点：腾讯接口验证",
                    "查询时间": datetime.now().strftime("%H:%M:%S")
                }
    except: return None
    return None

# --- 3. 界面展示 ---

if check_password():
    st.title("🚀 游资核心追踪 (13日回调-全腾讯无AK版)")

    thread_count = st.sidebar.slider("并发线程数", 1, 50, 30)
    
    if st.button("全市场极速穿透"):
        if 'scan_results' in st.session_state:
            del st.session_state['scan_results']
            
        with st.spinner("🚀 正在通过原生接口构建名单池..."):
            df_pool = get_tencent_pool()
            if df_pool.empty:
                st.error("数据接口暂时失联，请检查网络或刷新页面。")
                st.stop()
            
            # 严格过滤 (母版约束)
            df_pool = df_pool[~df_pool['名称'].str.contains("ST|退市")]
            df_pool = df_pool[~df_pool['代码'].str.startswith(("30", "68", "9"))]
            df_pool = df_pool[df_pool['换手率'] >= 3.0]

        stocks = df_pool.values.tolist()
        st.info(f"📊 名单：{len(stocks)} 只 | 引擎：腾讯 ifzq 原生接口")
        
        progress_bar = st.progress(0.0)
        results = []

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_stock = {executor.submit(process_single_stock, s[0], s[1], s[2], s[4], "全市场"): s for s in stocks}
            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res: 
                    results.append(res)
                    st.toast(f"✅ 捕获: {res['名称']}")
                
                if (i + 1) % 10 == 0 or (i+1) == len(stocks):
                    progress_bar.progress(float((i + 1) / len(stocks)))

        st.success(f"✨ 扫描完成！发现符合 13 日回调标的 {len(results)} 只")
        st.session_state['scan_results'] = results

    if 'scan_results' in st.session_state and st.session_state['scan_results']:
        res_df = pd.DataFrame(st.session_state['scan_results'])
        res_df.insert(0, '序号', range(1, len(res_df) + 1))
        st.divider()
        st.dataframe(res_df.style.set_properties(**{'text-align': 'center'}), use_container_width=True, hide_index=True)

        output = io.BytesIO()
        res_df.to_excel(output, index=False)
        st.download_button("📥 导出结果", data=output.getvalue(), file_name="腾讯选股结果.xlsx")

    st.divider()
    st.caption("Master Copy | 序号居中 | 纯腾讯接口 | 彻底去除 Akshare")
