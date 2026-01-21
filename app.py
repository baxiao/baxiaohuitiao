import streamlit as st
import baostock as bs
import pandas as pd
import time
import re
import random
from datetime import datetime, timedelta

# --- 页面配置 ---
st.set_page_config(page_title="A股全市场涨停回调筛选(Baostock版)", layout="wide")
st.title("🔍 A股全市场涨停回调筛选工具 (Baostock数据源)")

# --- 侧边栏设置 ---
st.sidebar.header="⚙️ 筛选参数设置"
days_to_fetch = st.sidebar.slider("获取历史天数", min_value=30, max_value=180, value=60)
limit_threshold = st.sidebar.slider("涨停阈值 (%)", min_value=9.0, max_value=20.0, value=9.9, step=0.1)

# 控制扫描速度
scan_speed = st.sidebar.selectbox("扫描速度 (越慢越稳)", options=["极速 (易断连)", "平衡 (推荐)", "龟速 (最稳)"], index=1)

if scan_speed == "极速 (易断连)":
    min_sleep, max_sleep = 0.2, 0.5
elif scan_speed == "平衡 (推荐)":
    min_sleep, max_sleep = 0.5, 1.0
else:
    min_sleep, max_sleep = 1.0, 2.0

st.sidebar.warning(f"提示：当前模式下，每只股票请求间隔为 {min_sleep}-{max_sleep} 秒。全市场扫描约需 {(5000*1.0)/60:.0f} 分钟。")

# --- 核心工具：带重试的请求 ---
def safe_baostock_request(func, max_retries=3, *args, **kwargs):
    for attempt in range(max_retries):
        try:
            rs = func(*args, **kwargs)
            if rs.error_code != '0':
                if attempt < max_retries - 1:
                    time.sleep((attempt + 1))
                    continue
                else:
                    st.error(f"Baostock Error: {rs.error_msg}")
                    return None
            return rs
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                st.error(f"Baostock 请求异常: {e}")
                return None
    return None

# --- 获取股票列表 (Baostock) ---
def get_stock_list():
    try:
        st.info("正在通过 Baostock 获取全市场 A 股列表...")
        
        # 登录 Baostock
        lg = bs.login()
        if lg.error_code != '0':
            st.error(f"Baostock 登录失败: {lg.error_msg}")
            return pd.DataFrame()
            
        # 获取证券信息
        rs = bs.query_all_stock(day=datetime.now().strftime("%Y-%m-%d"))
        
        if rs.error_code != '0':
            st.error(f"获取股票列表失败: {rs.error_msg}")
            bs.logout()
            return pd.DataFrame()
        
        # 转为 DataFrame
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
            
        bs.logout()
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # Baostock 列名: code, code_name, ipoDate, outDate, type, status
        # type: 1=股票, 2=指数; status: 1=正常上市, 0=终止上市
        
        # 过滤: 只保留股票(type=1) 且 正常上市(status=1)
        df = df[(df['type'] == '1') & (df['status'] == '1')]
        
        # 剔除 ST
        # Baostock 返回的 code_name 里包含 ST 信息
        df = df[~df['code_name'].str.contains('ST|退|PT|暂停')]
        
        # 只保留沪深A股 (sh.6xxxx, sz.0xxxx, sz.3xxxx)
        # Baostock 的 code 带有 sh. 或 sz. 前缀
        df = df[df['code'].str.match(r'^(sh\.6|sz\.[03])')]
        
        # 重命名
        df.rename(columns={'code': 'code', 'code_name': 'name'}, inplace=True)
        
        # 去掉前缀方便后续处理 (Baostock查历史也需要带前缀，这里暂时保留，或者后面统一处理)
        # 实际上 Baostock 查询历史也需要带前缀，所以这里保留 code 格式如 sh.600000
        
        st.success(f"获取成功，共筛选出 {len(df)} 只有效股票。")
        return df
        
    except Exception as e:
        st.error(f"获取列表过程出错: {e}")
        return pd.DataFrame()

# --- 策略分析函数 (Baostock) ---
@st.cache_data
def analyze_single_stock(code, name, end_date_str, history_days, threshold):
    try:
        # Baostock 日期格式: yyyy-MM-dd
        # 我们传入的 end_date_str 是 yyyyMMdd，需要转换
        end_date = datetime.strptime(end_date_str, "%Y%m%d")
        start_date = end_date - timedelta(days=history_days + 20)
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        # 登录 (每次查询最好重新登录以确保连接活跃，或者复用连接)
        lg = bs.login()
        if lg.error_code != '0':
            return None
            
        # 获取 K 线数据
        # frequency: d=日k线
        # adjustflag: 3=后复权 (类似 qfq)
        rs = bs.query_history_k_data_plus(
            code, 
            "date,open,high,low,close,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
            start_date=start_str, 
            end_date=end_str, 
            frequency="d", 
            adjustflag="3"
        )
        
        if rs.error_code != '0':
            bs.logout()
            return None
            
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
            
        bs.logout()
        
        if not data_list:
            return None
            
        df = pd.DataFrame(data_list, columns=rs.fields)
        
        # 数据清洗
        df['date'] = pd.to_datetime(df['date'])
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['pctChg'] = df['pctChg'].astype(float)
        
        df.set_index('date', inplace=True)
        
        # 定义涨停 (注意：Baostock 返回的 pctChg 是百分比字符串，已经转为 float 了，例如 9.98)
        is_limit_up = df['pctChg'] >= threshold
        
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
                    'current_price': df.loc[latest_date, 'close'],
                    'obs_end_date': obs_end.date()
                })

        # --- 策略1: 10天内出现两根涨停阳线 ---
        window_size = 10
        for i in range(len(df) - window_size):
            window = df.iloc[i : i + window_size]
            window_ups = window[window['pctChg'] >= threshold]
            
            if len(window_ups) >= 2:
                first_up = window_ups.index[0]
                
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
                        'current_price': df.loc[latest_date, 'close'],
                        'obs_end_date': obs_end.date()
                    })
        
        return results if results else None

    except Exception:
        return None

# --- 主程序 ---
stock_df = get_stock_list()

if not stock_df.empty:
    col1, col2 = st.columns([2, 1])
    
    if col1.button("🚀 开始全市场筛选", type="primary"):
        st.session_state['scan_results'] = []
        st.session_state['scanning'] = True
        
    if st.session_state.get('scanning', False):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        all_results = []
        total_stocks = len(stock_df)
        
        today_str = datetime.now().strftime("%Y%m%d")
        
        for index, row in stock_df.iterrows():
            code = row['code'] # Baostock code 带 sh. 或 sz.
            name = row['name']
            
            sleep_time = random.uniform(min_sleep, max_sleep)
            time.sleep(sleep_time)
            
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
        # 这里的 code 包含 sh. 或 sz.，显示时可以去掉前缀更美观
        stock_options = result_df.apply(lambda x: f"{x['name']} ({x['code'].split('.')[-1]})", axis=1).tolist()
        selected_stock = st.selectbox("选择一只股票查看K线", stock_options)
        
        if selected_stock:
            # 提取原始 code (带前缀)
            # 通过显示文本匹配
            display_name_part = selected_stock.split(' (')[0]
            # 在 result_df 中找到对应的原始 code
            original_code = result_df[result_df['name'] == display_name_part].iloc[0]['code']
            
            try:
                import mplfinance as mpf
                
                # 重新获取该股票数据进行绘图 (这里复用之前的逻辑，但数据源是 Baostock)
                # 为了绘图方便，我们还是重新查一次 Baostock
                lg = bs.login()
                if lg.error_code == '0':
                    rs = bs.query_history_k_data_plus(
                        original_code,
                        "date,open,high,low,close,volume",
                        start_date=(datetime.now()-timedelta(days=60)).strftime("%Y-%m-%d"),
                        end_date=datetime.now().strftime("%Y-%m-%d"),
                        frequency="d",
                        adjustflag="3"
                    )
                    data_list = []
                    while (rs.error_code == '0') & rs.next():
                        data_list.append(rs.get_row_data())
                    bs.logout()
                    
                    if data_list:
                        plot_df = pd.DataFrame(data_list, columns=rs.fields)
                        plot_df['date'] = pd.to_datetime(plot_df['date'])
                        plot_df.set_index('date', inplace=True)
                        plot_df = plot_df.astype(float)
                        
                        # Baostock 的 OHLC 列名是小写，mplfinance 默认是大写，需要重命名
                        plot_df.rename(columns={'open':'Open', 'high':'High', 'low':'Low', 'close':'Close', 'volume':'Volume'}, inplace=True)
                        
                        mc = mpf.make_marketcolors(up='r', down='g', edge='i', wick='i', volume='in', inherit=True)
                        s = mpf.make_mpf_style(marketcolors=mc, gridstyle='--')
                        
                        fig, axes = mpf.plot(plot_df, type='candle', style=s, returnfig=True, figsize=(14, 7))
                        st.pyplot(fig)
                    else:
                        st.error("无绘图数据")
                else:
                    st.error("Baostock 登录失败")
                    
            except Exception as e:
                st.error(f"绘图失败: {e}")
