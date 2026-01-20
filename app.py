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
st.set_page_config(page_title="游资核心追踪-Yahoo版", layout="wide")

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

# --- 2. 核心判定逻辑 (Yahoo Finance 适配版) ---

def get_beijing_time():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def is_limit_up(close, pre_close):
    """主板涨停判定"""
    if pd.isna(pre_close) or pre_close == 0: return False
    return close >= round(pre_close * 1.10 - 0.01, 2)

def process_single_stock(code, name, current_price, turnover_rate, sector_info):
    try:
        # Yahoo Finance 代码转换：60xxxx.SS (沪市) 或 00xxxx.SZ (深市)
        yf_code = f"{code}.SS" if code.startswith("60") else f"{code}.SZ"
        
        # 获取最近 40 天数据 (yfinance 获取速度极快且稳定)
        ticker = yf.Ticker(yf_code)
        hist = ticker.history(period="40d")
        
        if len(hist) < 25: return None
        
        hist = hist.reset_index()
        hist['pre_close'] = hist['Close'].shift(1)
        # 判定涨停
        hist['is_zt'] = hist.apply(lambda x: is_limit_up(x['Close'], x['pre_close']), axis=1)
        
        # --- 严格判定：仅筛选回调第 13 天 ---
        # 索引 -1 是今天，-14 是 13 个交易日前
        target_idx = len(hist) - 14
        if target_idx < 0: return None
        
        # 判定：13天前那根必须刚好是涨停阳线
        if hist.loc[target_idx, 'is_zt'] and hist.loc[target_idx, 'Close'] > hist.loc[target_idx, 'Open']:
            
            # 统计回调期间的涨停数
            after_slice = hist.loc[target_idx + 1 :, 'is_zt']
            zt_count_after = after_slice.sum()
            
            res_type = ""
            if zt_count_after > 0:
                # 功能 1: 10 天内双涨停
                ten_day_window = hist.loc[target_idx + 1 : target_idx + 10, 'is_zt']
                if ten_day_window.any():
                    res_type = "10天双涨停-仅回调13天"
            
            if not res_type and zt_count_after == 0:
                # 功能 2: 单次涨停
                res_type = "单次涨停-仅回调13天"
            
            # 状态判定：符合类型且今天未涨停
            if res_type and not hist.iloc[-1]['is_zt']:
                return {
                    "代码": code, "名称": name, "当前价格": f"{current_price:.2f}", 
                    "换手率": f"{turnover_rate}%", "判定强度": res_type, 
                    "智能决策": "Yahoo接口验证：精准13日周期",
                    "所属板块": sector_info, "查询时间": get_beijing_time()
                }
    except: return None
    return None

# --- 3. 页面渲染 (母版框架) ---

if check_password():
    st.title("🚀 游资核心追踪 (13日回调-Yahoo Finance版)")

    # 仅使用 akshare 获取板块和个股池列表（这步压力极小，通常不会封）
    @st.cache_data(ttl=3600)
    def get_market_data():
        try:
            sectors = ak.stock_board_industry_name_em()['板块名称'].tolist()
            return sectors
        except: return []

    all_sectors = get_market_data()
    selected_sector = st.sidebar.selectbox("选择查询范围", ["全市场扫描"] + all_sectors)
    thread_count = st.sidebar.slider("并发线程数 (Yahoo版建议20+)", 1, 50, 30)
    
    if st.button("开始穿透扫描"):
        if 'scan_results' in st.session_state:
            del st.session_state['scan_results']
            
        # 倒计时模块
        countdown = st.empty()
        for i in range(3, 0, -1):
            countdown.metric("Yahoo Finance 全球数据引擎预热...", f"{i} 秒")
            time.sleep(1)
        countdown.empty()

        with st.spinner("正在初始化股票池..."):
            try:
                if selected_sector == "全市场扫描":
                    df_pool = ak.stock_zh_a_spot_em()
                else:
                    df_pool = ak.stock_board_industry_cons_em(symbol=selected_sector)
                
                # 严格过滤 (母版核心)
                df_pool = df_pool[~df_pool['名称'].str.contains("ST|退市")]
                df_pool = df_pool[~df_pool['代码'].str.startswith(("30", "68", "9"))]
                df_pool = df_pool[df_pool['换手率'] >= 3.0]
            except:
                st.error("初始化失败，请重试")
                st.stop()

        stocks_to_check = df_pool[['代码', '名称', '最新价', '换手率']].values.tolist()
        total_stocks = len(stocks_to_check)
        st.info(f"📊 待扫：{total_stocks} 只 (使用 Yahoo Finance 接口)")
        
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        results = []

        # 多线程高压扫描 (Yahoo 接口抗压能力极强)
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_stock = {executor.submit(process_single_stock, s[0], s[1], s[2], s[3], selected_sector): s for s in stocks_to_check}
            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res: 
                    results.append(res)
                    st.toast(f"✅ Yahoo捕获: {res['名称']}")
                
                if (i + 1) % 10 == 0 or (i+1) == total_stocks:
                    progress_bar.progress(float((i + 1) / total_stocks))
                    status_text.text(f"🚀 扫描进度: {i+1}/{total_stocks}")

        status_text.success(f"✨ 扫描完成！共发现 {len(results)} 只标的")
        st.session_state['scan_results'] = results

    # 结果展示 (序号居中)
    if 'scan_results' in st.session_state and st.session_state['scan_results']:
        res_df = pd.DataFrame(st.session_state['scan_results'])
        res_df.insert(0, '序号', range(1, len(res_df) + 1))
        st.divider()
        st.dataframe(
            res_df.style.set_properties(**{'text-align': 'center'}), 
            use_container_width=True, 
            hide_index=True
        )

        output = io.BytesIO()
        res_df.to_excel(output, index=False)
        st.download_button(label="📥 导出 Excel", data=output.getvalue(), file_name=f"Yahoo选股_{datetime.now().strftime('%m%d')}.xlsx")

    st.divider()
    st.caption("Master Copy | 序号居中稳定版 | Yahoo Finance 接口驱动")
