import streamlit as st
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="2026-01-14 序号居中稳定母版", layout="wide")

def fetch_data_ak(code, name):
    """单只股票逻辑判断：13日内仅一次涨停"""
    try:
        # 获取个股历史行情 (Akshare 速度极快)
        df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
        if len(df) < 15: return None
        
        # 截取最近 14 天
        recent = df.tail(14).copy()
        # 计算涨幅 (Akshare 返回的数据通常自带涨跌幅，但手动计算更稳)
        recent['pct_chg'] = (recent['收盘'] - recent['前收']) / recent['前收'] * 100
        
        # 核心逻辑：有且仅有一次涨停 (>= 9.8%)
        limit_up_mask = recent['pct_chg'] >= 9.8
        if limit_up_mask.sum() == 1:
            limit_up_idx = recent[limit_up_mask].index[0]
            # 计算距今天数
            days_passed = (len(df) - 1) - limit_up_idx
            return {
                "代码": code, "名称": name, 
                "现价": recent.iloc[-1]['收盘'], 
                "今日涨幅": f"{round(recent.iloc[-1]['pct_chg'], 2)}%",
                "距涨停天数": days_passed
            }
    except:
        return None
    return None

def main():
    st.title("📊 单次涨停回调筛选器 (Akshare 极速版)")
    st.info("规则：剔除 ST/创业板/科创板 | 13日内仅一次涨停 | 序号居中稳定母版")

    # 1. 操作区
    col1, col2 = st.columns([1, 4])
    with col1:
        run_btn = st.button("🚀 开始极速筛选")
    
    if run_btn:
        # 2. 获取全市场实时清单
        with st.spinner("正在获取全 A 股清单..."):
            try:
                stock_list_df = ak.stock_zh_a_spot_em()
                # 执行母本过滤规则
                # 剔除 ST
                stock_list_df = stock_list_df[~stock_list_df['名称'].str.contains("ST|st")]
                # 剔除 创业板(300)、科创板(688)
                stock_list_df = stock_list_df[~stock_list_df['代码'].str.startswith(('300', '688'))]
                
                stocks = stock_list_df[['代码', '名称']].values.tolist()
            except Exception as e:
                st.error(f"获取列表失败: {e}")
                return

        # 3. 多线程加速
        final_results = []
        progress_bar = st.progress(0)
        status = st.empty()
        
        total = len(stocks)
        # Akshare 不需要登录，线程可以开到 15-20
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {executor.submit(fetch_data_ak, s[0], s[1]): s for s in stocks}
            
            for i, future in enumerate(as_completed(futures)):
                res = future.result()
                if res:
                    final_results.append(res)
                
                # 每 50 只更新一次进度，减少页面刷新
                if i % 50 == 0:
                    progress_bar.progress((i + 1) / total)
                    status.text(f"已扫描 {i+1}/{total} 只个股...")

        status.success(f"筛选完成！共发现 {len(final_results)} 只个股符合条件。")
        progress_bar.empty()

        # 4. 展示与导出
        if final_results:
            df_res = pd.DataFrame(final_results)
            # 序号居中稳定处理
            df_res.index = range(1, len(df_res) + 1)
            st.dataframe(df_res, use_container_width=True)
            
            # 导出功能
            csv = df_res.to_csv(index=True).encode('utf-8-sig')
            st.download_button("📥 导出结果为 Excel(CSV)", csv, "single_limit_up_callback.csv", "text/csv")
        else:
            st.warning("当前行情下，未发现符合“单次涨停+13日回调”的个股。")

if __name__ == "__main__":
    main()
