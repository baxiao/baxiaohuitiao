import streamlit as st
import pandas as pd
import baostock as bs
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta
import gc

# --- 1. 页面配置 ---
st.set_page_config(page_title="游资核心追踪-极简版", layout="wide")

# --- 2. 核心检测函数 ---

def check_baostock():
    """检测 Baostock 连接是否正常"""
    try:
        lg = bs.login()
        if lg.error_code == '0':
            # 尝试拉取一个简单数据验证权限
            rs = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
            bs.logout()
            if rs.error_code == '0':
                return True
        return False
    except:
        return False

# --- 3. 分析逻辑引擎 ---

def fetch_analysis(bs_code, name):
    """单股穿透逻辑 (换手率 + 连阳)"""
    try:
        rs = bs.query_history_k_data_plus(bs_code,
            "date,open,high,low,close,volume,turnover",
            start_date=(datetime.now() - timedelta(days=35)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d'),
            frequency="d", adjustflag="3")
        
        if rs is None or rs.error_code != '0': return None

        data = []
        while rs.next(): data.append(rs.get_row_data())
        if len(data) < 8: return None
        
        df = pd.DataFrame(data, columns=["date","open","high","low","close","volume","turnover"])
        df[['open','high','low','close','volume','turnover']] = df[['open','high','low','close','volume','turnover']].apply(pd.to_numeric)
        
        # 环节二：换手率 ≥ 3%
        latest_turnover = df.iloc[-1]['turnover']
        if latest_turnover >= 3.0:
            # 环节三：连阳判定
            df['is_pos'] = df['close'] > df['open']
            pos_list = df['is_pos'].tolist()
            if len(pos_list) >= 8 and all(pos_list[-8:]): return None

            for d, g_limit in [(7, 22.5), (6, 17.5), (5, 12.5)]:
                sub = df.tail(d)
                if (sub['close'] > sub['open']).all():
                    gain = round(((sub.iloc[-1]['close'] - sub.iloc[0]['open']) / sub.iloc[0]['open']) * 100, 2)
                    if gain <= g_limit:
                        return {
                            "代码": bs_code.split('.')[1], "名称": name, 
                            "换手率": f"{latest_turnover}%", "判定": f"{d}连阳", 
                            "涨幅": f"{gain}%", "收盘价": round(df.iloc[-1]['close'], 2)
                        }
        return None
    except: return None

# --- 4. 界面渲染 ---

with st.sidebar:
    st.header("🛠️ 系统状态")
    
    # 1. 接口自检展示
    with st.spinner("检查中..."):
        is_ok = check_baostock()
        if is_ok:
            st.success("📈 Baostock 数据接口: 正常")
        else:
            st.error("❌ Baostock 数据接口: 异常")
            st.button("重试检测")
    
    st.divider()
    st.header("⚙️ 扫描控制")
    thread_num = st.slider("并发强度", 1, 10, 5)
    st.caption("注：若遇卡顿请调低强度")

st.title("🚀 游资核心追踪")

if st.button("启动全量扫描") and is_ok:
    bs.login()
    
    # 步骤1：获取全量主板名单
    with st.spinner("获取名单中..."):
        rs_all = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
        stock_list = []
        while rs_all.next():
            row = rs_all.get_row_data()
            if row[0].startswith(('sh.60', 'sz.00')) and "ST" not in row[1]:
                stock_list.append([row[0], row[1]])
    
    if stock_list:
        # 步骤2：执行分层筛选
        final_results = []
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        
        # 采用小批量处理，防内存溢出
        batch_size = 200
        for i in range(0, len(stock_list), batch_size):
            batch = stock_list[i : i + batch_size]
            with ThreadPoolExecutor(max_workers=thread_num) as executor:
                futures = {executor.submit(fetch_analysis, s[0], s[1]): s for s in batch}
                for j, future in enumerate(as_completed(futures)):
                    try:
                        res = future.result(timeout=4)
                        if res: final_results.append(res)
                    except: continue
                    
                    # 更新进度
                    total_done = i + j + 1
                    progress_bar.progress(total_done / len(stock_list))
                    status_text.text(f"进度: {total_done} / {len(stock_list)}")
            gc.collect() # 强行释放内存

        # 结果展示
        st.divider()
        if final_results:
            df_final = pd.DataFrame(final_results)
            df_final.insert(0, '序号', range(1, len(df_final) + 1))
            st.subheader(f"🏆 精选分析报表 (命中 {len(final_results)} 只)")
            st.dataframe(df_final, use_container_width=True, hide_index=True)
            
            output = io.BytesIO()
            df_final.to_excel(output, index=False)
            st.download_button("📥 导出报表", output.getvalue(), "精选清单.xlsx")
        else:
            st.warning("完成扫描，未发现符合条件的标的。")
    
    bs.logout()
else:
    if not is_ok:
        st.warning("⚠️ 请先等待接口检测正常后再启动。")

st.divider()
st.caption("2026-01-21 | Baostock 驱动 | 稳定性优先版")
