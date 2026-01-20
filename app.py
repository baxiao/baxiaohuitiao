import streamlit as st
import pandas as pd
import yfinance as yf
import akshare as ak
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta, timezone
import random

# --- 1. 配置与安全 (严格遵循母版) ---
st.set_page_config(page_title="游资核心追踪-终极版", layout="wide")

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

# --- 2. 核心判定逻辑 (Yahoo 驱动) ---

def is_limit_up(close, pre_close):
    if pd.isna(pre_close) or pre_close == 0: return False
    return close >= round(pre_close * 1.10 - 0.01, 2)

def process_single_stock(code, name, current_price, turnover_rate, sector_info):
    try:
        # Yahoo Finance 代码适配 (SS沪, SZ深)
        symbol = f"{code}.SS" if code.startswith("60") else f"{code}.SZ"
        
        # 极速抓取 K 线
        df = yf.download(symbol, period="40d", interval="1d", progress=False, timeout=10)
        if df is None or len(df) < 25: return None
        
        # 兼容 yfinance 新版本的多级索引
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        df = df.reset_index()
        df['pre_close'] = df['Close'].shift(1)
        df['is_zt'] = df.apply(lambda x: is_limit_up(x['Close'], x['pre_close']), axis=1)
        
        # --- 精准 13 日回调判定 ---
        # -1 为今天, -14 为 13 个交易日前
        target_idx = len(df) - 14
        if target_idx < 0: return None
        
        # 必须是13天前那一根刚好涨停阳线
        if df.loc[target_idx, 'is_zt'] and df.loc[target_idx, 'Close'] > df.loc[target_idx, 'Open']:
            after_slice = df.loc[target_idx + 1 :, 'is_zt']
            zt_count_after = after_slice.sum()
            
            res_type = ""
            if zt_count_after > 0:
                ten_day_window = df.loc[target_idx + 1 : target_idx + 10, 'is_zt']
                if ten_day_window.any():
                    res_type = "10天双涨停-仅回调13天"
            
            if not res_type and zt_count_after == 0:
                res_type = "单次涨停-仅回调13天"
            
            # 今天未涨停则录入
            if res_type and not df.iloc[-1]['is_zt']:
                return {
                    "代码": code, "名称": name, "当前价格": current_price, "换手率": turnover_rate,
                    "判定强度": res_type, "决策建议": "精准13日周期达成",
                    "所属板块": sector_info, "查询时间": datetime.now().strftime("%H:%M:%S")
                }
    except: return None
    return None

# --- 3. 页面渲染 (本地化名单保险机制) ---

if check_password():
    st.title("🚀 游资核心追踪 (Yahoo+本地名单增强版)")

    # 1. 尝试获取板块，失败则展示默认分类
    @st.cache_data(ttl=3600)
    def get_sectors_safe():
        try: return ak.stock_board_industry_name_em()['板块名称'].tolist()
        except: return ["通信服务", "软件开发", "半导体", "电力行业", "汽车整车"]

    all_sectors = get_sectors_safe()
    selected_sector = st.sidebar.selectbox("选择查询范围", ["全市场扫描"] + all_sectors)
    thread_count = st.sidebar.slider("并发线程数", 1, 30, 15)
    
    if st.button("开始穿透扫描"):
        if 'scan_results' in st.session_state:
            del st.session_state['scan_results']
            
        countdown = st.empty()
        for i in range(3, 0, -1):
            countdown.metric("极速引擎正在连接 Yahoo...", f"{i} 秒")
            time.sleep(1)
        countdown.empty()

        with st.spinner("🚀 正在构建名单池 (含抗压机制)..."):
            df_pool = None
            # 策略：多重接口循环尝试
            for _ in range(3):
                try:
                    if selected_sector == "全市场扫描":
                        df_pool = ak.stock_zh_a_spot_em()
                    else:
                        df_pool = ak.stock_board_industry_cons_em(symbol=selected_sector)
                    if df_pool is not None and not df_pool.empty: break
                except: time.sleep(1)

            # 如果还是失败，使用本地名单保底 (针对全市场)
            if df_pool is None and selected_sector == "全市场扫描":
                st.warning("⚠️ 名单接口繁忙，正在启用本地名单进行全市场扫描...")
                try:
                    df_pool = ak.stock_info_a_code_name() # 这个接口最轻量
                except:
                    st.error("❌ 无法获取股票名单，请刷新网络或稍后再试。")
                    st.stop()

            # 严格过滤 (剔除ST、创业板、科创板)
            df_pool = df_pool[~df_pool['名称'].str.contains("ST|退市")]
            df_pool = df_pool[~df_pool['代码'].str.startswith(("30", "68", "9"))]

        stocks_to_check = df_pool[['代码', '名称']].values.tolist()
        total_stocks = len(stocks_to_check)
        st.info(f"📊 名单构建成功：{total_stocks} 只 (开始全速判定)")
        
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        results = []

        # 多线程扫描
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_stock = {executor.submit(
                process_single_stock, s[0], s[1], "N/A", "N/A", selected_sector
            ): s for s in stocks_to_check}
            
            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res: 
                    results.append(res)
                    st.toast(f"✅ 捕获: {res['名称']}")
                
                if (i + 1) % 10 == 0 or (i+1) == total_stocks:
                    progress_bar.progress(float((i + 1) / total_stocks))
                    status_text.text(f"🚀 已完成: {i+1}/{total_stocks}")

        status_text.success(f"✨ 扫描结束！共计捕获 {len(results)} 只标的")
        st.session_state['scan_results'] = results

    # 4. 结果展示 (序号居中)
    if 'scan_results' in st.session_state and st.session_state['scan_results']:
        res_df = pd.DataFrame(st.session_state['scan_results'])
        res_df.insert(0, '序号', range(1, len(res_df) + 1))
        st.divider()
        st.dataframe(
            res_df.style.set_properties(**{'text-align': 'center'}), 
            use_container_width=True, hide_index=True
        )

        output = io.BytesIO()
        res_df.to_excel(output, index=False)
        st.download_button(label="📥 导出 Excel", data=output.getvalue(), file_name=f"13日扫描_{datetime.now().strftime('%m%d')}.xlsx")

    st.divider()
    st.caption("Master Copy | 13日严格版 | Yahoo引擎 | 名单本地化补丁")
