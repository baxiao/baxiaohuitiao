import streamlit as st
import akshare as ak
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta

# --- 配置页面 ---
st.set_page_config(page_title="A股涨停回调分析工具", layout="wide")
st.title("🚀 A股涨停回调分析工具 (基于Streamlit)")
st.markdown("""
**功能说明：**
1. **模式1**：筛选10天内出现两根涨停阳线，标记从首根阳线次日开始的13天观察期。
2. **模式2**：标记单次涨停个股隔日起的13天观察期。
""")

# --- 侧边栏设置 ---
st.sidebar.header("参数设置")
stock_code = st.sidebar.text_input("股票代码", value="600519", max_chars=6, help="例如：600519 (贵州茅台)")
start_date = st.sidebar.date_input("开始日期", datetime.now() - timedelta(days=180))
end_date = st.sidebar.date_input("结束日期", datetime.now())

# --- 数据获取函数 ---
@st.cache_data
def get_stock_data(code, start, end):
    try:
        # 获取A股前复权数据
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end, adjust="qfq")
        if df.empty:
            return None
        
        # 数据清洗
        df['日期'] = pd.to_datetime(df['日期'])
        df.set_index('日期', inplace=True)
        # 重命名列以适配 mplfinance
        df.rename(columns={
            '开盘': 'Open', '最高': 'High', '最低': 'Low', 
            '收盘': 'Close', '成交量': 'Volume'
        }, inplace=True)
        
        # 计算涨跌幅 (用于辅助判断)
        df['pct_change'] = df['Close'].pct_change()
        return df
    except Exception as e:
        st.error(f"获取数据失败: {e}")
        return None

# --- 策略核心逻辑 ---
def analyze_signals(df):
    if df is None or df.empty:
        return None

    # 定义涨停 (这里简化为涨幅 >= 9.9%，实际ST股是5%，科创板20%，可根据需要细化)
    # 为了演示，我们使用通用的大于9.8%
    is_limit_up = df['pct_change'] >= 0.098
    limit_up_days = df[is_limit_up].index

    signals = []
    
    # 策略1: 10天内出现两根涨停阳线，以首根阳线第二天开始回调13天
    # 逻辑：找到所有满足条件的时间段
    # 这里我们简化逻辑：如果在10天窗口内有2个以上涨停，则标记第一个涨停后的13天
    
    window_days = 10
    callback_days = 13
    
    # 遍历数据，寻找符合条件的窗口
    for i in range(len(df) - window_days):
        window_df = df.iloc[i : i + window_days]
        window_limit_ups = window_df[window_df['pct_change'] >= 0.098]
        
        if len(window_limit_ups) >= 2:
            # 找到了符合条件的窗口
            first_up_date = window_limit_ups.index[0]
            second_up_date = window_limit_ups.index[1]
            
            # 标记区域：首根次日 -> +13天
            start_mark = first_up_date + timedelta(days=1)
            end_mark = first_up_date + timedelta(days=callback_days)
            
            # 避免重复标记（简单去重）
            if not any(s['date'] == first_up_date for s in signals):
                signals.append({
                    'type': '双涨停模式',
                    'date': first_up_date,
                    'start_highlight': start_mark,
                    'end_highlight': end_mark,
                    'desc': f"10日双连阳，回调观察期：{start_mark.date()} 至 {end_mark.date()}"
                })

    # 策略2: 单次涨停个股隔日起回调13天 (为了不覆盖策略1，我们优先显示策略1，或者只显示非重叠的)
    # 这里逻辑：只要是涨停，就标记后13天
    for date in limit_up_days:
        start_mark = date + timedelta(days=1)
        end_mark = date + timedelta(days=callback_days)
        
        # 检查这个时间是否已经被策略1覆盖，避免太乱，可选逻辑
        signals.append({
            'type': '单次涨停',
            'date': date,
            'start_highlight': start_mark,
            'end_highlight': end_mark,
            'desc': f"单日涨停，观察期：{start_mark.date()} 至 {end_mark.date()}"
        })

    return pd.DataFrame(signals)

# --- 绘图函数 ---
def plot_chart(df, signals_df, code):
    if df.empty:
        return

    # 准备绘图数据
    mc = mpf.make_marketcolors(up='r', down='g', edge='i', wick='i', volume='in', inherit=True)
    s = mpf.make_mpf_style(marketcolors=mc, gridstyle='--', y_on_right=False)

    # 创建叠加图层
    addplot_list = []

    # 如果有信号，添加矩形标记
    if signals_df is not None and not signals_df.empty:
        fig, axes = mpf.plot(df, type='candle', style=s, returnfig=True, figsize=(14, 8))
        ax = axes[0]
        
        # 倒序遍历以免重叠遮挡太严重，或者只画最近的
        for _, row in signals_df.tail(5).iterrows(): # 只画最近5个信号，避免图太花
            start = row['start_highlight']
            end = row['end_highlight']
            
            # 确保日期在数据范围内
            if start < df.index[-1] and end > df.index[0]:
                color = 'yellow' if row['type'] == '双涨停模式' else 'blue'
                alpha = 0.2
                
                # 使用 axvspan 绘制背景区域
                ax.axvspan(start, end, color=color, alpha=alpha, label=row['type'])
                
                # 在图上标注文字
                ax.text(start, df.loc[start, 'High'] * 1.02, row['type'], fontsize=9, color=color)

        st.pyplot(fig)
    else:
        fig, axes = mpf.plot(df, type='candle', style=s, returnfig=True, figsize=(14, 8))
        st.pyplot(fig)

# --- 主程序执行 ---
if st.button("开始分析"):
    data = get_stock_data(stock_code, start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"))
    
    if data is not None:
        st.subheader(f"股票代码: {stock_code} K线图")
        
        signals = analyze_signals(data)
        
        # 绘制图表
        plot_chart(data, signals, stock_code)
        
        # 显示信号列表
        st.subheader("📅 发现的信号列表")
        if signals is not None and not signals.empty:
            # 优先显示双涨停模式
            dual_mode = signals[signals['type'] == '双涨停模式']
            single_mode = signals[signals['type'] == '单次涨停']
            
            if not dual_mode.empty:
                st.markdown("#### 🔴 重点：双涨停回调信号")
                st.dataframe(dual_mode[['date', 'type', 'desc']].sort_values(by='date', ascending=False), use_container_width=True)
            
            if not single_mode.empty:
                with st.expander("查看所有单次涨停信号"):
                    st.dataframe(single_mode[['date', 'type', 'desc']].sort_values(by='date', ascending=False), use_container_width=True)
        else:
            st.info("在选定时间范围内未发现符合条件的涨停信号。")
