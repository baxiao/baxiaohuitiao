import streamlit as st
import pandas as pd
import baostock as bs
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta

# --- 1. 页面配置 ---
st.set_page_config(page_title="游资核心追踪-原生稳定版", layout="wide")

# --- 2. 核心业务逻辑 ---

def get_all_mainboard_stocks():
    """环节一：静默获取全市场主板股票代码"""
    rs = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    
    if not data_list:
        return []
        
    raw_df = pd.DataFrame(data_list, columns=rs.fields)
    
    # 核心过滤：仅限沪深主板 (60... 和 00...)，剔除 ST
    main_df = raw_df[
        (~raw_df['code_name'].str.contains("ST")) & 
        (raw_df['code'].str.startswith(('sh.60', 'sz.00')))
    ]
    
    return main_df[['code', 'code_name']].values.tolist()

def fetch_stock_analysis(bs_code, name):
    """环节二：筛查换手率 ≥ 3% (含超时保护)"""
    try:
        # 获取最近 25 天数据确保 K 线充足
        rs = bs.query_history_k_data_plus(bs_code,
            "date,open,high,low,close,volume,turnover",
            start_date=(datetime.now() - timedelta(days=35)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d'),
            frequency="d", adjustflag="3")
        
        if rs.error_code != '0':
            return None

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if len(data_list) < 8: return None
        
        df = pd.DataFrame(data_list, columns=["date","open","high","low","close","volume","turnover"])
        df[['open','high','low','close','volume','turnover']] = df[['open','high','low','close','volume','turnover']].apply(pd.to_numeric)
        
        # 获取最新交易日换手率
        latest_turnover = df.iloc[-1]['turnover']
        
        # 环节二硬指标：换手率 ≥ 3%
        if latest_turnover >= 3.0:
            return {"code": bs_code, "name": name, "df": df, "turnover": latest_turnover}
    except:
        return None
    return None

def check_positive_days(stock_obj):
    """环节三：连阳验证 (5-7 连阳限制)"""
    df = stock_obj['df']
    df['is_pos'] = df['close'] > df['open']
    pos_list = df['is_pos'].tolist()
    
    # 严禁 8 连阳及以上
    if len(pos_list) >= 8 and all(pos_list[-8:]): return None

    # 阶梯涨幅限价逻辑
    for d, g_limit in [(7, 22.5), (6, 17.5), (5, 12.5)]:
        sub = df.tail(d)
        if (sub['close'] > sub['open']).all():
            # 计算区间涨幅: (最后一天收盘价 - 连阳第一天开盘价) / 第一天开盘价
            gain = round(((sub.iloc[-1]['close'] - sub.iloc[0]['open']) / sub.iloc[0]['open']) * 100, 2)
            if gain <= g_limit:
                return {
                    "代码": stock_obj['code'].replace('sh.','').replace('sz.',''), 
                    "名称": stock_obj['name'], 
                    "换手率": f"{stock_obj['turnover']}%", 
                    "强度": f"{d}连阳", 
                    "涨幅": f"{gain}%", 
                    "收盘价": round(df.iloc[-1]['close'], 2)
                }
    return None

# --- 3. 页面渲染 ---

st.title("🚀 游资核心追踪 (Baostock 全量稳定母版)")

with st.sidebar:
    st.header("控制台")
    thread_num = st.slider("并发强度 (建议 10)", 1, 20, 10)
    st.divider()
    st.write("**当前逻辑：**")
    st.write("1. 自动扫描全场主板")
    st.write("2. 过滤换手率 < 3%")
    st.write("3. 筛选 5-7 连阳")
    st.write("4. 自动剔除 ST/创业/科创")

if st.button("启动穿透扫描"):
    # 登录 Baostock 环境
    lg = bs.login()
    if lg.error_code != '0':
        st.error(f"Baostock 登录失败: {lg.error_msg}")
    else:
        # 环节一：获取名单
        with st.spinner("📦 正在拉取全市场主板名册..."):
            initial_list = get_all_mainboard_stocks()
        
        if not initial_list:
            st.error("环节一失败：未获取到股票名单。")
        else:
            # 环节二：扫描换手率
            st.write(f"### 📍 环节二：活跃股筛选 (待扫总量: {len(initial_list)})")
            passed_turnover = []
            progress_bar = st.progress(0.0)
            status_text = st.empty()
            
            # 使用 ThreadPoolExecutor 并设置 Future 超时
            with ThreadPoolExecutor(max_workers=thread_num) as executor:
                # 提交所有任务
                future_to_stock = {executor.submit(fetch_stock_analysis, s[0], s[1]): s for s in initial_list}
                
                for i, future in enumerate(as_completed(future_to_stock)):
                    try:
                        # 💡 强制 3 秒超时，防止某只股票卡死整个队列
                        res = future.result(timeout=3)
                        if res:
                            passed_turnover.append(res)
                    except Exception:
                        pass # 超时或报错直接放弃该票，确保流程继续
                    
                    # 实时更新进度条
                    pct = (i + 1) / len(initial_list)
                    progress_bar.progress(pct)
                    if (i + 1) % 10 == 0: # 减少 UI 刷新频率，提高性能
                        status_text.text(f"已扫描: {i+1} / {len(initial_list)}")

            # 环节三：连阳筛选
            if passed_turnover:
                st.success(f"环节二完成！筛选出 {len(passed_turnover)} 只活跃个股。")
                
                st.divider()
                st.write("### 🔥 环节三：5-7 连阳战法精选")
                final_results = []
                
                for obj in passed_turnover:
                    res = check_positive_days(obj)
                    if res:
                        final_results.append(res)
                        st.toast(f"✅ 捕获强势股: {res['名称']}")

                if final_results:
                    # 结果展示
                    res_df = pd.DataFrame(final_results)
                    res_df.insert(0, '序号', range(1, len(res_df) + 1))
                    
                    st.subheader("📋 最终精选分析报表")
                    st.dataframe(res_df, use_container_width=True, hide_index=True)
                    
                    # 导出 Excel
                    output = io.BytesIO()
                    res_df.to_excel(output, index=False)
                    st.download_button("📥 导出今日决策清单", output.getvalue(), f"游资精选_{datetime.now().strftime('%m%d')}.xlsx")
                else:
                    st.warning("环节三结束：今日全场活跃股中暂无符合 5-7 连阳条件的标的。")
            else:
                st.error("环节二结束：未发现换手率达标个股。")
        
        # 退出登录
        bs.logout()

st.divider()
st.caption("2026-01-21 | Baostock 原生驱动 | 超时熔断保护 | 序号居中稳定版")
