import streamlit as st
import akshare as ak
import pandas as pd
import time
import re
import random
from datetime import datetime, timedelta

# --- 页面配置 ---
st.set_page_config(page_title="A股全市场涨停回调筛选(终极版)", layout="wide")
st.title("🔍 A股全市场涨停回调筛选工具 (自动适配列名版)")

# --- 侧边栏设置 ---
st.sidebar.header="⚙️ 筛选参数设置"
days_to_fetch = st.sidebar.slider("获取历史天数", min_value=30, max_value=180, value=60)
limit_threshold = st.sidebar.slider("涨停阈值 (%)", min_value=9.0, max_value=20.0, value=9.9, step=0.1)

# 控制扫描速度
scan_speed = st.sidebar.selectbox("扫描速度 (越慢越稳)", options=["极速 (易断连)", "平衡 (推荐)", "龟速 (最稳)"], index=1)

if scan_speed == "极速 (易断连)":
    min_sleep, max_sleep = 0.1, 0.3
elif scan_speed == "平衡 (推荐)":
    min_sleep, max_sleep = 0.5, 1.0
else:
    min_sleep, max_sleep = 1.0, 2.0

st.sidebar.warning(f"提示：当前模式下，每只股票请求间隔为 {min_sleep}-{max_sleep} 秒。全市场扫描约需 {(5000*1.5)/60:.0f} 分钟。")

# --- 核心工具：智能列名修复 ---
def standardize_columns(df):
    """无论列名是中文还是英文，统一转换为 code 和 name"""
    if df.empty:
        return df
    
    new_cols = {}
    for col in df.columns:
        col_str = str(col)
        if 'code' in col_str.lower() or '代码' in col_str:
            new_cols[col] = 'code'
        elif 'name' in col_str.lower() or '名称' in col_str:
            new_cols[col] = 'name'
    
    if not new_cols:
        return df # 无法识别，原样返回
        
    df = df.rename(columns=new_cols)
    
    # 尝试只保留这两列，防止冲突
    if 'code' in df.columns and 'name' in df.columns:
        return df[['code', 'name']]
    return df

# --- 核心工具：带重试的请求 ---
def safe_request(func, max_retries=3, *args, **kwargs):
    """执行函数，如果失败则重试"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                # st.warning(f"请求失败 ({e})，{wait_time}秒后重试... (第 {attempt+1}/{max_retries} 次)") # 这里的warning在循环里会刷屏，注释掉或用日志
                time.sleep(wait_time)
            else:
                return None
    return None

# --- 获取股票列表 (分离获取 + 智能列名适配) ---
def get_stock_list():
    try:
        st.info("正在分别获取沪市和深市股票列表...")
        
        # 1. 获取沪市
        sh_list = safe_request(ak.stock_info_sh_name_code)
        if sh_list is None or sh_list.empty:
            st.error("获取沪市列表失败，可能是网络波动。")
            return pd.DataFrame()
        sh_list = standardize_columns(sh_list)
        
        # 2. 获取深市
        sz_list = safe_request(ak.stock_info_sz_name_code)
        if sz_list is None or sz_list.empty:
            st.error("获取深市列表失败，可能是网络波动。")
            return pd.DataFrame()
        sz_list = standardize_columns(sz_list)
        
        # 检查是否成功转换列名
        if 'code' not in sh_list.columns or 'name' not in sh_list.columns:
            st.error("沪市数据列名解析失败，请联系开发者更新。")
            return pd.DataFrame()
            
        # 3. 合并
        all_stocks = pd.concat([sh_list, sz_list], ignore_index=True)
        
        # 4. 数据清洗
        all_stocks['code'] = all_stocks['code'].astype(str).str.zfill(6)
        all_stocks['name'] = all_stocks['name'].astype(str)
        
        # 5. 剔除 ST, *ST, 退, 停, PT
        # 这里必须确保 name 列存在
        if 'name' in all_stocks.columns:
            # 使用正则匹配，更精准
            pattern = re.compile(r'^(\*?ST|ST|退|PT)')
            filtered = all_stocks[~all_stocks['name'].str.match(pattern)]
        else:
            # 极端情况保护
            filtered = all_stocks
            
        # 6. 剔除 B 股 (代码通常带 .SH 后缀或者纯数字的 900xxx)
        # 这里简单判断，如果代码里有点，或者是900开头，可能是B股
        filtered = filtered[~filtered['code'].str.contains('\.')]
        filtered = filtered[~filtered['code'].str.startswith('900')]
        
        st.success(f"获取成功，共筛选出 {len(filtered)} 只有效股票。")
        return filtered
    except Exception as e:
        st.error(f"获取列表过程出错: {e}")
        return pd.DataFrame()

# --- 策略分析函数 ---
@st.cache_data
def analyze_single_stock(code, name, end_date_str, history_days, threshold):
    try:
        end_date = datetime.strptime(end_date_str, "%Y%m%d")
        start_date = end_date - timedelta(days=history_days + 20) 
        start_str = start_date.strftime("%Y%m%d")
        
        # 获取数据
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_str, end_date=end_date_str, adjust="qfq")
        except:
            return None
        
        if df.empty or len(df) < 20:
            return None
            
        # 统一列名 (以防万一)
        df.rename(columns={'开盘':'Open', '收盘':'Close', '最高':'High', '最低':'Low', '成交量':'Volume'}, inplace=True)
        
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.set_index('日期').sort_index()
        df['pct_change'] = df['Close'].pct_change()
        
        # 定义涨停
        is_limit_up = df['pct_change'] >= (threshold / 100.0)
        
        latest_date = df.index[-1]
        results = []
        
        # --- 策略2: 单次涨停隔日起回调13天 ---
        limit_dates = df[is_limit_up].index
        
        for date in limit_dates:
            obs_start = date + timedelta(days=1)
            obs_end = date + timedelta(days=13)
            
            if obs_start <= latest_date <= obs_end:
                results.append({
                    'code': code,
                    'name': name,
                    'type': '单次涨停观察中',
                    'trigger_date': date.date(),
                    'days_into_pullback': (latest_date - date).days,
                    'current_price': df.loc[latest_date, 'Close'],
                    'obs_end_date': obs_end.date()
                })

        # --- 策略1: 10天内出现两根涨停阳线 ---
        window_size = 10
        for i in range(len(df) - window_size):
            window = df.iloc[i : i + window_size]
            window_ups = window[window['pct_change'] >= (threshold / 100.0)]
            
            if len(window_ups) >= 2:
                first_up = window_ups.index[0]
                
                # 避免重复
                already_added = any(r['trigger_date'] == first_up.date() and r['type'] == '🔥 双涨停模式' for r in results)
                if already_added:
                    continue
                
                obs_start = first_up + timedelta(days=1)
                obs_end = first_up + timedelta(days=13)
                
                if obs_start <= latest_date <= obs_end:
                    results.append({
                        'code': code,
                        'name': name,
                        'type': '🔥 双涨停模式',
                        'trigger_date': first_up.date(),
                        'days_into_pullback': (latest_date - first_up).days,
                        'current_price': df.loc[latest_date, 'Close'],
                        'obs_end_date': obs_end.date()
                    })
        
        return results if results else None

    except Exception:
        return None

# --- 主程序 ---
stock_df = get_stock_list()

if not stock_df.empty:
    col1, col2 = st.columns([2, 1])
    
    if col1.button("🚀 开始稳健全市场筛选", type="primary"):
        st.session_state['scan_results'] = []
        st.session_state['scanning'] = True
        
    if st.session_state.get('scanning', False):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        all_results = []
        total_stocks = len(stock_df)
        
        today_str = datetime.now().strftime("%Y%m%d")
        
        for index, row in stock_df.iterrows():
            code = row['code']
            name = row['name']
            
            # 随机延时
            sleep_time = random.uniform(min_sleep, max_sleep)
            time.sleep(sleep_time)
            
            # 更新进度
            progress = (index + 1) / total_stocks
            progress_bar.progress(progress)
            status_text.text(f"正在分析: {name} ({code}) - 进度: {int(progress*100)}%")
            
            res = analyze_single_stock(code, name, today_str, days_to_fetch, limit_threshold)
            if res:
                all_results.extend(res)
            
        st.session_state['scanning'] = False
        st.session_state['scan_results'] = all_results
        progress_bar.empty()
        status_text.text("✅ 扫描完成！")
        
        if all_results:
            st.success(f"共发现 {len(all_results)} 个符合观察条件的信号。")
            st.session_state['result_df'] = pd.DataFrame(all_results)
        else:
            st.warning("在当前参数下未发现符合条件的目标股票。")

# --- 结果展示 ---
if 'scan_results' in st.session_state and st.session_state['scan_results']:
    result_df = st.session_state['result_df']
    
    tab1, tab2 = st.tabs(["📊 筛选结果列表", "📈 详细K线图"])
    
    with tab1:
        st.subheader(f"发现 {len(result_df)} 个符合观察条件的信号")
        
        dual_mode = result_df[result_df['type'] == '🔥 双涨停模式']
        single_mode = result_df[result_df['type'] == '单次涨停观察中']
        
        if not dual_mode.empty:
            st.markdown("### 🔴 重点：双涨停回调观察")
            st.dataframe(dual_mode.sort_values(by='days_into_pullback', ascending=True), use_container_width=True)
            
        if not single_mode.empty:
            st.markdown("### 🔵 普通单涨停观察")
            st.dataframe(single_mode.sort_values(by='days_into_pullback', ascending=True), use_container_width=True)
            
        csv = result_df.to_csv(index=False).encode('utf-8')
        st.download_button("下载CSV结果", csv, "stock_signals.csv", "text/csv")

    with tab2:
        st.subheader("查看个股详情")
        stock_options = result_df.apply(lambda x: f"{x['name']} ({x['code']})", axis=1).tolist()
        selected_stock = st.selectbox("选择一只股票查看K线", stock_options)
        
        if selected_stock:
            code = selected_stock.split('(')[1].split(')')[0]
            try:
                import mplfinance as mpf
                
                plot_df = safe_request(
                    ak.stock_zh_a_hist, 
                    symbol=code, period="daily", 
                    start_date=(datetime.now()-timedelta(days=60)).strftime("%Y%m%d"), 
                    end_date=datetime.now().strftime("%Y%m%d"), 
                    adjust="qfq"
                )
                
                if plot_df is not None and not plot_df.empty:
                    plot_df['日期'] = pd.to_datetime(plot_df['日期'])
                    plot_df.set_index('日期', inplace=True)
                    plot_df.rename(columns={'开盘':'Open', '最高':'High', '最低':'Low', '收盘':'Close', '成交量':'Volume'}, inplace=True)
                    
                    mc = mpf.make_marketcolors(up='r', down='g', edge='i', wick='i', volume='in', inherit=True)
                    s = mpf.make_mpf_style(marketcolors=mc, gridstyle='--')
                    
                    fig, axes = mpf.plot(plot_df, type='candle', style=s, returnfig=True, figsize=(14, 7))
                    st.pyplot(fig)
                else:
                    st.error("无法获取该股票K线数据，可能网络波动或该股票停牌。")
                    
            except Exception as e:
                st.error(f"绘图失败: {e}")
