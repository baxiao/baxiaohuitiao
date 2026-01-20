import streamlit as st
import pandas as pd
import akshare as ak
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta, timezone
import random

# --- 1. 配置与安全 (严格遵循母版: Secrets读取, 不设默认密码) ---
st.set_page_config(page_title="游资核心追踪-13日严格版", layout="wide")

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        pwd = st.text_input("请输入访问令牌", type="password")
        if st.button("验证登录"):
            # 严格遵循母版设置，从 Secrets 中读取
            target_pwd = st.secrets.get("STOCK_SCAN_PWD")
            if target_pwd is None:
                st.error("配置错误：请在 Secrets 中设置 STOCK_SCAN_PWD")
                return False
            if pwd == target_pwd:
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("令牌错误")
        return False
    return True

# --- 2. 核心判定逻辑 (精准修改判定内核) ---

def get_beijing_time():
    tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

def is_limit_up(close, pre_close):
    """主板涨停判定逻辑"""
    if pd.isna(pre_close) or pre_close == 0: return False
    return close >= round(pre_close * 1.10 - 0.01, 2)

def process_single_stock(code, name, current_price, turnover_rate, sector_info):
    try:
        # --- 稳定性补丁：随机呼吸时间，防止 Connection Reset ---
        time.sleep(random.uniform(0.2, 0.5))
        
        # 抓取数据
        hist = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq").tail(35)
        if hist is None or len(hist) < 20: return None
        
        hist = hist.reset_index(drop=True)
        hist.columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
        hist['pre_close'] = hist['收盘'].shift(1)
        hist['is_zt'] = hist.apply(lambda x: is_limit_up(x['收盘'], x['pre_close']), axis=1)
        
        # --- 严格判定：仅筛选回调第13天的标的 ---
        # 索引 -1 是今天，-14 是 13 个交易日前
        target_idx = len(hist) - 14
        if target_idx < 0: return None
        
        # 判定：13天前那根【必须】是涨停阳线
        if hist.loc[target_idx, 'is_zt'] and hist.loc[target_idx, '收盘'] > hist.loc[target_idx, '开盘']:
            
            # 统计从那根涨停次日至今的所有涨停数
            after_slice = hist.loc[target_idx + 1 :, 'is_zt']
            zt_count_after = after_slice.sum()
            
            res_type = ""
            # 情况1：10天内双停（首根后回调13天）
            if zt_count_after > 0:
                ten_day_window = hist.loc[target_idx + 1 : target_idx + 10, 'is_zt']
                if ten_day_window.any():
                    res_type = "10天双涨停-仅限回调13天"
            
            # 情况2：单次涨停（隔日起仅回调13天）
            if not res_type and zt_count_after == 0:
                res_type = "单次涨停-仅限回调13天"
            
            # 必须今天未涨停（处于回调状态）且类型符合
            if res_type and not hist.iloc[-1]['is_zt']:
                return {
                    "代码": code, "名称": name, "当前价格": current_price, "换手率": turnover_rate,
                    "判定强度": res_type, "智能决策": "精准13日周期：文字直接展示",
                    "所属板块": sector_info, "查询时间": get_beijing_time()
                }
    except: return None
    return None

# --- 3. 页面渲染 (保持母版框架：多线程、倒计时、板块过滤、Excel导出) ---

if check_password():
    st.title("🚀 游资核心追踪 (13日回调精准筛选版)")

    # 板块选择
    with st.spinner("同步实时数据..."):
        try:
            all_sectors = ak.stock_board_industry_name_em()['板块名称'].tolist()
        except:
            all_sectors = []
            
    selected_sector = st.sidebar.selectbox("选择查询范围", ["全市场扫描"] + all_sectors)
    # 并发数微调为15，更适合云端抗压
    thread_count = st.sidebar.slider("并发线程数", 1, 30, 15)
    
    if st.button("开始穿透扫描"):
        if 'scan_results' in st.session_state:
            del st.session_state['scan_results']
            
        # 母版标志性倒计时
        countdown = st.empty()
        for i in range(3, 0, -1):
            countdown.metric("极速引擎正在预热...", f"{i} 秒")
            time.sleep(1)
        countdown.empty()

        with st.spinner("正在筛选池标的..."):
            df_pool = None
            for _ in range(3):
                try:
                    if selected_sector == "全市场扫描":
                        df_pool = ak.stock_zh_a_spot_em()
                    else:
                        df_pool = ak.stock_board_industry_cons_em(symbol=selected_sector)
                    break
                except: time.sleep(2)
            
            if df_pool is None:
                st.error("接口连接繁忙，请1分钟后重试")
                st.stop()

            # 严格过滤：剔除ST、创业板、科创板
            df_pool = df_pool[~df_pool['名称'].str.contains("ST|退市")]
            df_pool = df_pool[~df_pool['代码'].str.startswith(("30", "68", "9"))]
            df_pool = df_pool[df_pool['换手率'] >= 3.0]

        stocks_to_check = df_pool[['代码', '名称', '最新价', '换手率']].values.tolist()
        total_stocks = len(stocks_to_check)
        st.info(f"📊 待扫标的：{total_stocks} 只 (仅看回调第13天)")
        
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        results = []

        # 多线程扫描
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_stock = {executor.submit(process_single_stock, s[0], s[1], s[2], s[3], selected_sector): s for s in stocks_to_check}
            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res: 
                    results.append(res)
                    st.toast(f"✅ 捕获: {res['名称']}")
                
                if (i + 1) % 10 == 0 or (i+1) == total_stocks:
                    progress_bar.progress(float((i + 1) / total_stocks))
                    status_text.text(f"🚀 扫描进度: {i+1}/{total_stocks}")

        status_text.success(f"✨ 扫描完成！仅录入13日回调个股：{len(results)} 只")
        st.session_state['scan_results'] = results

    # 结果展示 (序号居中，文字直接显示)
    if 'scan_results' in st.session_state and st.session_state['scan_results']:
        res_df = pd.DataFrame(st.session_state['scan_results'])
        res_df.insert(0, '序号', range(1, len(res_df) + 1))
        
        st.divider()
        st.subheader("📋 扫描分析结果")
        st.dataframe(
            res_df.style.set_properties(**{'text-align': 'center'}), 
            use_container_width=True, 
            hide_index=True
        )

        output = io.BytesIO()
        res_df.to_excel(output, index=False)
        st.download_button(
            label="📥 导出结果 (Excel)",
            data=output.getvalue(),
            file_name=f"13日精准选股_{datetime.now().strftime('%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.divider()
    st.caption("Master Copy | 序号居中稳定母版框架 | 严格仅限13日回调逻辑")
