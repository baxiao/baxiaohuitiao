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
st.set_page_config(page_title="游资核心追踪-Yahoo增强版", layout="wide")

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

# --- 2. 核心判定逻辑 (Yahoo Finance 适配) ---

def get_beijing_time():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def is_limit_up(close, pre_close):
    """主板 10% 涨停判定"""
    if pd.isna(pre_close) or pre_close == 0: return False
    return close >= round(pre_close * 1.10 - 0.01, 2)

def process_single_stock(code, name, current_price, turnover_rate, sector_info):
    try:
        # Yahoo Finance 代码适配
        symbol = f"{code}.SS" if code.startswith("60") else f"{code}.SZ"
        
        # 抓取最近 40 天 K 线
        df = yf.download(symbol, period="40d", interval="1d", progress=False)
        if df is None or len(df) < 25: return None
        
        df = df.reset_index()
        # 处理 Yahoo 多级索引列名问题
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        df['pre_close'] = df['Close'].shift(1)
        df['is_zt'] = df.apply(lambda x: is_limit_up(x['Close'], x['pre_close']), axis=1)
        
        # --- 精准 13 日回调判定 ---
        # 索引 -1 为今天，-14 为 13 个交易日前的那根 K 线
        target_idx = len(df) - 14
        if target_idx < 0: return None
        
        # 13 天前必须是涨停阳线
        if df.loc[target_idx, 'is_zt'] and df.loc[target_idx, 'Close'] > df.loc[target_idx, 'Open']:
            
            # 统计之后到今天的涨停数
            after_slice = df.loc[target_idx + 1 :, 'is_zt']
            zt_count_after = after_slice.sum()
            
            res_type = ""
            # 逻辑 A：10 天内双涨停
            if zt_count_after > 0:
                ten_day_window = df.loc[target_idx + 1 : target_idx + 10, 'is_zt']
                if ten_day_window.any():
                    res_type = "10天双涨停-仅回调13天"
            
            # 逻辑 B：单次涨停
            if not res_type and zt_count_after == 0:
                res_type = "单次涨停-仅回调13天"
            
            # 必须今天未涨停（处于回调状态）
            if res_type and not df.iloc[-1]['is_zt']:
                return {
                    "代码": code, "名称": name, "当前价格": current_price, "换手率": turnover_rate,
                    "判定强度": res_type, "智能决策": "Yahoo数据源：精准13日周期达成",
                    "所属板块": sector_info, "查询时间": get_beijing_time()
                }
    except: return None
    return None

# --- 3. 页面渲染 (抗造版初始化) ---

if check_password():
    st.title("🚀 游资核心追踪 (Yahoo接口稳定版)")

    # 获取板块列表：增加多次尝试机制
    @st.cache_data(ttl=3600)
    def fetch_sectors_safe():
        for _ in range(3):
            try:
                return ak.stock_board_industry_name_em()['板块名称'].tolist()
            except: time.sleep(1)
        return ["全市场扫描"]

    all_sectors = fetch_sectors_safe()
    selected_sector = st.sidebar.selectbox("选择查询范围", ["全市场扫描"] + all_sectors)
    thread_count = st.sidebar.slider("并发线程数", 1, 30, 20)
    
    if st.button("开始穿透扫描"):
        if 'scan_results' in st.session_state:
            del st.session_state['scan_results']
            
        countdown = st.empty()
        for i in range(3, 0, -1):
            countdown.metric("Yahoo Finance 全球数据中心连接中...", f"{i} 秒")
            time.sleep(1)
        countdown.empty()

        with st.spinner("🚀 正在通过母版接口提取池标的..."):
            df_pool = None
            # 强化初始化：尝试从不同接口拿名单
            for _ in range(3):
                try:
                    if selected_sector == "全市场扫描":
                        df_pool = ak.stock_zh_a_spot_em()
                    else:
                        df_pool = ak.stock_board_industry_cons_em(symbol=selected_sector)
                    if df_pool is not None and not df_pool.empty: break
                except:
                    time.sleep(2)
            
            if df_pool is None:
                st.warning("⚠️ 母版名单接口超时，正在尝试备用实时名单...")
                try:
                    df_pool = ak.stock_info_a_code_name() # 备用接口
                except:
                    st.error("❌ 所有初始化接口均繁忙，请刷新页面重试。")
                    st.stop()

            # 严格过滤 (母版核心)
            df_pool = df_pool[~df_pool['名称'].str.contains("ST|退市")]
            df_pool = df_pool[~df_pool['代码'].str.startswith(("30", "68", "9"))]
            # 确保有换手率字段，没有则跳过过滤
            if '换手率' in df_pool.columns:
                df_pool = df_pool[df_pool['换手率'] >= 3.0]

        stocks_to_check = df_pool[['代码', '名称']].values.tolist()
        # 兼容处理价格和换手率显示
        price_map = dict(zip(df_pool['代码'], df_pool.get('最新价', [0]*len(df_pool))))
        turnover_map = dict(zip(df_pool['代码'], df_pool.get('换手率', [0]*len(df_pool))))

        total_stocks = len(stocks_to_check)
        st.info(f"📊 待扫池：{total_stocks} 只 (Yahoo 数据全速抓取中)")
        
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        results = []

        # 多线程高压扫描
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_stock = {executor.submit(
                process_single_stock, s[0], s[1], price_map.get(s[0], 0), turnover_map.get(s[0], 0), selected_sector
            ): s for s in stocks_to_check}
            
            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res: 
                    results.append(res)
                    st.toast(f"✅ 捕获: {res['名称']}")
                
                if (i + 1) % 10 == 0 or (i+1) == total_stocks:
                    progress_bar.progress(float((i + 1) / total_stocks))
                    status_text.text(f"🚀 扫描进度: {i+1}/{total_stocks}")

        status_text.success(f"✨ 扫描完成！本次精准录入 {len(results)} 只标的")
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
    st.caption("Master Copy | 序号居中稳定版 | 严格仅限13日回调 | Yahoo 引擎")
