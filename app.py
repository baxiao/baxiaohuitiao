import streamlit as st
import pandas as pd
import baostock as bs
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta, timezone
import openai

# --- 1. 配置 ---
st.set_page_config(page_title="游资核心追踪-直连版", layout="wide")

# --- 2. 核心业务引擎 ---

def get_initial_pool(keyword=""):
    """环节一：静默获取初始名单 (DeepSeek)"""
    api_key = st.secrets.get("DEEPSEEK_API_KEY")
    if not api_key:
        st.error("❌ 未在 Secrets 中配置 DEEPSEEK_API_KEY")
        return []
    
    client = openai.OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    target = f"“{keyword}”题材" if keyword else "全市场近期成交活跃"
    prompt = f"请列出A股主板中{target}的股票名单。格式：代码,名称。仅限60或00开头，剔除ST。不要有废话。"
    
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3
        )
        lines = response.choices[0].message.content.strip().split('\n')
        return [l.split(',') for l in lines if ',' in l and l.split(',')[0].strip().startswith(('60','00'))]
    except Exception as e:
        st.error(f"AI 寻源失败: {e}")
        return []

def fetch_baostock_data(code, name):
    """环节二：使用 Baostock 筛查换手率"""
    try:
        bs_code = f"sh.{code}" if code.startswith("60") else f"sz.{code}"
        rs = bs.query_history_k_data_plus(bs_code,
            "date,open,high,low,close,volume,turnover",
            start_date=(datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),
            end_date=datetime.now().strftime('%Y-%m-%d'),
            frequency="d", adjustflag="3")
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if len(data_list) < 8: return None
        
        df = pd.DataFrame(data_list, columns=["date","open","high","low","close","volume","turnover"])
        df[['open','high','low','close','volume','turnover']] = df[['open','high','low','close','volume','turnover']].apply(pd.to_numeric)
        
        latest_turnover = df.iloc[-1]['turnover']
        if latest_turnover >= 3.0:
            return {"code": code, "name": name, "df": df, "turnover": latest_turnover}
    except:
        return None
    return None

def check_positive_days(stock_obj):
    """环节三：连阳验证"""
    df = stock_obj['df']
    df['is_pos'] = df['close'] > df['open']
    pos_list = df['is_pos'].tolist()
    
    if len(pos_list) >= 8 and all(pos_list[-8:]): return None

    for d, g_limit in [(7, 22.5), (6, 17.5), (5, 12.5)]:
        sub = df.tail(d)
        if (sub['close'] > sub['open']).all():
            gain = round(((sub.iloc[-1]['close'] - sub.iloc[0]['open']) / sub.iloc[0]['open']) * 100, 2)
            if gain <= g_limit:
                return {
                    "代码": stock_obj['code'], "名称": stock_obj['name'], 
                    "换手率": f"{stock_obj['turnover']}%", "判定强度": f"{d}连阳", 
                    "区间涨幅": f"{gain}%", "收盘价": round(df.iloc[-1]['close'], 2)
                }
    return None

# --- 3. 页面渲染 ---

st.title("🚀 游资核心追踪 (直连扫描版)")

with st.sidebar:
    st.header("扫描设置")
    keyword = st.text_input("题材关键词 (留空则全扫)", value="")
    thread_num = st.slider("并发强度", 1, 10, 5)
    st.info("提示：此版本已移除密码验证，直接启动。")

if st.button("开始穿透扫描"):
    bs.login()
    
    # 环节一：静默寻源
    with st.spinner("🤖 环节一：AI 锁定初始池..."):
        initial_list = get_initial_pool(keyword)
    
    if initial_list:
        # 环节二：筛查换手率
        st.write(f"### 📍 环节二：活跃股筛选 (换手率 ≥ 3%)")
        passed_turnover = []
        progress_1 = st.progress(0.0)
        
        with ThreadPoolExecutor(max_workers=thread_num) as executor:
            futures = {executor.submit(fetch_baostock_data, s[0].strip(), s[1].strip()): s for s in initial_list}
            for i, f in enumerate(as_completed(futures)):
                res = f.result()
                if res: passed_turnover.append(res)
                progress_1.progress((i + 1) / len(initial_list))
        
        if passed_turnover:
            turn_df = pd.DataFrame([{"代码": x['code'], "名称": x['name'], "换手率": f"{x['turnover']}%"} for x in passed_turnover])
            st.dataframe(turn_df, use_container_width=True, height=200)

            # 环节三：连阳验证
            st.divider()
            st.write(f"### 🔥 环节三：连阳战法精选")
            final_results = []
            for obj in passed_turnover:
                res = check_positive_days(obj)
                if res:
                    final_results.append(res)
                    st.toast(f"✅ 命中: {res['名称']}")

            if final_results:
                res_df = pd.DataFrame(final_results)
                res_df.insert(0, '序号', range(1, len(res_df) + 1))
                st.dataframe(res_df, use_container_width=True, hide_index=True)
                
                output = io.BytesIO()
                res_df.to_excel(output, index=False)
                st.download_button("📥 导出决策报告", output.getvalue(), "扫描结果.xlsx")
            else:
                st.warning("环节三结束：无符合 5-7 连阳条件的标的。")
        else:
            st.error("环节二结束：无符合换手率条件的标的。")
    
    bs.logout()

st.divider()
st.caption("2026-01-21 | 1.15 无密码版 | 核心驱动: Baostock")
