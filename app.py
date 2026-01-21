import streamlit as st
import akshare as ak
import pandas as pd
import time
import re
from datetime import datetime, timedelta

# --- 页面配置 ---
st.set_page_config(page_title="A股全市场涨停回调筛选", layout="wide")
st.title("🔍 A股全市场涨停回调筛选工具 (剔除ST/退市)")

# --- 侧边栏设置 ---
st.sidebar.header="⚙️ 筛选参数设置"
days_to_fetch = st.sidebar.slider("获取历史天数", min_value=30, max_value=180, value=60, help="获取多少天的数据进行分析")
limit_threshold = st.sidebar.slider("涨停阈值 (%)", min_value=9.0, max_value=20.0, value=9.9, step=0.1)

st.sidebar.info("注意：全市场筛选需要请求数千次API，首次运行较慢，请耐心等待。")

# --- 核心逻辑函数 ---

def get_stock_list():
    """获取A股所有股票代码，并剔除ST和退市股"""
    try:
        st.info("正在获取全市场股票列表...")
        stock_list = ak.stock_info_a_code_name()
        
        # 预处理：转为字符串并过滤
        stock_list['code'] = stock_list['code'].astype(str).str.zfill(6)
        
        # 过滤逻辑
        # 1. 剔除 ST, *ST, 退
        # 2. 只保留 6 (沪主板), 0 (深主板), 3 (创业板) - 可根据需要调整，这里包含创业板
        valid_pattern = re.compile(r'^(600|601|603|605|688|000|001|002|003|300)')
        
        filtered_list = stock_list[
            (~stock_list['name'].str.contains('ST|退|停')) & 
            (stock_list['code'].str.match(valid_pattern))
        ]
        
        st.success(f"获取成功，共筛选出 {len(filtered_list)} 只有效股票。")
        return filtered_list
    except Exception as e:
        st.error(f"获取股票列表失败: {e}")
        return pd.DataFrame()

@st.cache_data
def analyze_single_stock(code, name, end_date_str, history_days, threshold):
    """分析单只股票是否符合条件"""
    try:
        # 计算开始日期
        end_date = datetime.strptime(end_date_str, "%Y%m%d")
        start_date = end_date - timedelta(days=history_days + 20) # 多取一点确保有数据
        
        start_str = start_date.strftime("%Y%m%d")
        
        # 获取数据 (前复权)
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_str, end_date=end_date_str, adjust="qfq")
        
        if df.empty or len(df) < 20:
            return None
            
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.set_index('日期').sort_index()
        df['pct_change'] = df['收盘'].pct_change()
        
        # 定义涨停
        is_limit_up = df['pct_change'] >= (threshold / 100.0)
        
        # 获取最近的日期
        latest_date = df.index[-1]
        results = []
        
        # --- 策略2: 单次涨停隔日起回调13天 ---
        # 找出所有涨停日
        limit_dates = df[is_limit_up].index
        
        for date in limit_dates:
            # 观察区间：涨停次日 到 涨停日+13天
            # 只有当“今天”还在观察区间内时，才提示用户
            obs_start = date + timedelta(days=1)
            obs_end = date + timedelta(days=13)
            
            if obs_start <= latest_date <= obs_end:
                results.append({
                    'code': code,
                    'name': name,
                    'type': '单次涨停观察中',
                    'trigger_date': date.date(),
                    'days_into_pullback': (latest_date - date).days,
                    'current_price': df.loc[latest_date, '收盘'],
                    'obs_end_date': obs_end.date()
                })

        # --- 策略1: 10天内出现两根涨停阳线 ---
        # 滚动窗口检查
        window_size = 10
        for i in range(len(df) - window_size):
            window = df.iloc[i : i + window_size]
            window_ups = window[window['pct_change'] >= (threshold / 100.0)]
            
            if len(window_ups) >= 2:
                # 取首根涨停
                first_up = window_ups.index[0]
                
                # 检查是否重复 (防止同一次信号被重复记录)
                already_added = any(r['trigger_date'] == first_up.date() and r['type'] == '双涨停模式' for r in results)
                if already_added:
                    continue
                
                # 观察期逻辑：首根次日 -> +13天
                obs_start = first_up + timedelta(days=1)
                obs_end = first_up + timedelta(days=13)
                
                if obs_start <= latest_date <= obs_end:
                    results.append({
                        'code': code,
                        'name': name,
                        'type': '🔥 双涨停模式',
                        'trigger_date': first_up.date(),
                        'days_into_pullback': (latest_date - first_up).days,
                        'current_price': df.loc[latest_date, '收盘'],
                        'obs_end_date': obs_end.date()
                    })
        
        return results if results else None

    except Exception as e:
        # 忽略个别股票数据错误，以免打断整体循环
        return None

# --- 程序主体 ---

# 1. 获取股票列表
stock_df = get_stock_list()

if not stock_df.empty:
    col1, col2 = st.columns([2, 1])
    
    if col1.button("🚀 开始全市场筛选", type="primary"):
        # 初始化 Session State 存储结果
        st.session_state['scan_results'] = []
        st.session_state['scanning'] = True
        
    if st.session_state.get('scanning', False):
        # 创建进度条
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        all_results = []
        total_stocks = len(stock_df)
        
        # 今天的日期字符串
        today_str = datetime.now().strftime("%Y%m%d")
        
        # 遍历所有股票
        # 注意：这里为了演示流畅性，会稍微限制每次请求的间隔
        for index, row in stock_df.iterrows():
            code = row['code']
            name = row['name']
            
            # 更新进度
            progress = (index + 1) / total_stocks
            progress_bar.progress(progress)
            status_text.text(f"正在扫描: {name} ({code}) - 进度: {int(progress*100)}%")
            
            # 执行分析
            res = analyze_single_stock(code, name, today_str, days_to_fetch, limit_threshold)
            if res:
                all_results.extend(res)
            
            # 稍微延时，防止请求过快被封 IP
            time.sleep(0.05) 
            
        # 扫描完成
        st.session_state['scanning'] = False
        st.session_state['scan_results'] = all_results
        progress_bar.empty()
        status_text.text("扫描完成！")
        
        # 将结果存入 DataFrame
        if all_results:
            result_df = pd.DataFrame(all_results)
            st.session_state['result_df'] = result_df
        else:
            st.warning("未找到符合条件的目标股票。")

# --- 结果展示 ---
if 'scan_results' in st.session_state and st.session_state['scan_results']:
    result_df = st.session_state['result_df']
    
    # 标签页展示
    tab1, tab2 = st.tabs(["📊 筛选结果列表", "📈 详细K线图"])
    
    with tab1:
        st.subheader(f"发现 {len(result_df)} 个符合观察条件的信号")
        
        # 分类展示
        dual_mode = result_df[result_df['type'] == '🔥 双涨停模式']
        single_mode = result_df[result_df['type'] == '单次涨停观察中']
        
        if not dual_mode.empty:
            st.markdown("### 🔴 重点：双涨停回调观察")
            st.dataframe(dual_mode.sort_values(by='days_into_pullback', ascending=True), use_container_width=True)
            
        if not single_mode.empty:
            st.markdown("### 🔵 普通单涨停观察")
            st.dataframe(single_mode.sort_values(by='days_into_pullback', ascending=True), use_container_width=True)
            
        # 全量下载
        csv = result_df.to_csv(index=False).encode('utf-8')
        st.download_button("下载CSV结果", csv, "stock_signals.csv", "text/csv")

    with tab2:
        st.subheader("查看个股详情")
        # 股票选择器
        stock_options = result_df.apply(lambda x: f"{x['name']} ({x['code']})", axis=1).tolist()
        selected_stock = st.selectbox("选择一只股票查看K线", stock_options)
        
        if selected_stock:
            # 提取代码
            code = selected_stock.split('(')[1].split(')')[0]
            
            # 重新获取该股票数据画图 (复用之前的绘图逻辑，这里简化直接调用akshare)
            try:
                import mplfinance as mpf
                import matplotlib.pyplot as plt
                
                plot_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=(datetime.now()-timedelta(days=60)).strftime("%Y%m%d"), end_date=datetime.now().strftime("%Y%m%d"), adjust="qfq")
                plot_df['日期'] = pd.to_datetime(plot_df['日期'])
                plot_df.set_index('日期', inplace=True)
                plot_df.rename(columns={'开盘':'Open', '最高':'High', '最低':'Low', '收盘':'Close', '成交量':'Volume'}, inplace=True)
                
                # 绘图
                mc = mpf.make_marketcolors(up='r', down='g', edge='i', wick='i', volume='in', inherit=True)
                s = mpf.make_mpf_style(marketcolors=mc, gridstyle='--')
                
                fig, axes = mpf.plot(plot_df, type='candle', style=s, returnfig=True, figsize=(14, 7))
                st.pyplot(fig)
                
            except Exception as e:
                st.error(f"绘图失败: {e}")

