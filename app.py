import streamlit as st
import pandas as pd
import baostock as bs
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta
import gc

# --- 1. 配置 ---
st.set_page_config(page_title="游资核心追踪-加固版", layout="wide")

# --- 2. 核心分析逻辑 ---

def fetch_stock_analysis_safe(bs_code, name):
    """
    单只股票处理逻辑
    注意：此函数内部不再调用 bs.login()，由主程序统一维护连接
    """
    try:
        # 获取历史数据
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,volume,turnover",
            start_date=(datetime.now() - timedelta(days=35)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d'),
            frequency="d", adjustflag="3"
        )
        
        # 核心报错处理：如果返回错误码，说明连接可能已经断开
        if rs is None or rs.error_code != '0':
            return None

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        if len(data_list) < 8: return None
        
        df = pd.DataFrame(data_list, columns=["date","open","high","low","close","volume","turnover"])
        df[['open','high','low','close','volume','turnover']] = df[['open','high','low','close','volume','turnover']].apply(pd.to_numeric)
        
        latest_turnover = df.iloc[-1]['turnover']
        
        # 环节二：换手率过滤
        if latest_turnover >= 3.0:
            # 环节三：连阳判定
            df['is_pos'] = df['close'] > df['open']
            pos_list = df['is_pos'].tolist()
            
            # 剔除 8 连阳及以上
            if len(pos_list) >= 8 and all(pos_list[-8:]): return None

            for d, g_limit in [(7, 22.5), (6, 17.5), (5, 12.5)]:
                sub = df.tail(d)
                if (sub['close'] > sub['open']).all():
                    gain = round(((sub.iloc[-1]['close'] - sub.iloc[0]['open']) / sub.iloc[0]['open']) * 100, 2)
                    if gain <= g_limit:
                        return {
                            "代码": bs_code.split('.')[1], 
                            "名称": name, 
                            "换手率": f"{latest_turnover}%", 
                            "判定强度": f"{d}连阳", 
                            "区间涨幅": f"{gain}%", 
                            "最新价": round(df.iloc[-1]['close'], 2)
                        }
        return None
    except Exception:
        return None

# --- 3. 页面渲染 ---

st.title("🚀 游资核心追踪 (架构加固版)")

with st.sidebar:
    st.header("控制台")
    # 💡 强制将并发建议调低到 3-5，Baostock 的稳定性第一
    thread_num = st.slider("并发强度", 1, 8, 4)
    st.info("提示：此版本增强了连接保护，如遇中断将自动跳过。")

if st.button("启动全量穿透扫描"):
    # 统一登录
    login_res = bs.login()
    if login_res.error_code != '0':
        st.error(f"登录失败: {login_res.error_msg}")
    else:
        # 环节一：获取名册
        with st.spinner("📦 正在拉取全量名册..."):
            rs_all = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
            stock_list = []
            while rs_all.next():
                row = rs_all.get_row_data()
                # 过滤主板和非 ST
                if (row[0].startswith(('sh.60', 'sz.00'))) and ("ST" not in row[1]):
                    stock_list.append([row[0], row[1]])
        
        if stock_list:
            st.write(f"### 📍 环节二 & 三：全市场联合分析 (总量: {len(stock_list)})")
            final_results = []
            progress_bar = st.progress(0.0)
            status_text = st.empty()
            
            # 💡 采用分批处理模式，每 100 个强制检查一次连接
            batch_size = 100
            for i in range(0, len(stock_list), batch_size):
                batch = stock_list[i : i + batch_size]
                
                with ThreadPoolExecutor(max_workers=thread_num) as executor:
                    futures = {executor.submit(fetch_stock_analysis_safe, s[0], s[1]): s for s in batch}
                    for j, future in enumerate(as_completed(futures)):
                        try:
                            # 增加更短的超时，避免 Bad File Descriptor 扩散
                            res = future.result(timeout=5)
                            if res:
                                final_results.append(res)
                                st.toast(f"✅ 捕获: {res['名称']}")
                        except:
                            continue
                        
                        # 更新进度
                        total_done = i + j + 1
                        progress_bar.progress(total_done / len(stock_list))
                        if total_done % 20 == 0:
                            status_text.text(f"已处理: {total_done} / {len(stock_list)}")
                
                # 每组结束释放内存
                gc.collect()

            # 结果展示
            st.divider()
            if final_results:
                res_df = pd.DataFrame(final_results)
                res_df.insert(0, '序号', range(1, len(res_df) + 1))
                st.subheader("🏆 最终精选战报")
                st.dataframe(res_df, use_container_width=True, hide_index=True)
                
                output = io.BytesIO()
                res_df.to_excel(output, index=False)
                st.download_button("📥 导出全场扫描报告", output.getvalue(), "全量分析报告.xlsx")
            else:
                st.warning("完成扫描，未发现符合条件的标的。")
        
        # 统一登出
        bs.logout()

st.divider()
st.caption("2026-01-21 | Baostock 驱动 | 异常熔断机制 | 稳定性优先版")
