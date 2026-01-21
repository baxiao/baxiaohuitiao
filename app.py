import streamlit as st
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime
import requests
import json

# --- 1. 配置与安全 ---
st.set_page_config(page_title="游资核心追踪-纯腾讯引擎", layout="wide")

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        st.subheader("🛡️ 系统访问安全校验")
        pwd = st.text_input("请输入访问令牌", type="password")
        if st.button("验证并进入系统"):
            target_pwd = st.secrets.get("STOCK_SCAN_PWD")
            if target_pwd and pwd == target_pwd:
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("令牌校验失败")
        return False
    return True

# --- 2. 腾讯原生 API 数据引擎 (核心修复) ---

@st.cache_data(ttl=600)
def get_tencent_full_pool():
    """获取腾讯全量 A 股名单 (修复0只问题)"""
    # 腾讯的核心行情列表接口
    url = "https://gu.qq.com/proxy/squote/list?m=all&t=sh_a,sz_a&p=1&l=6000&v=2"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://gu.qq.com/"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        # 腾讯接口数据在 data['data']['list'] 中
        raw_list = data['data']['list']
        stocks = []
        for item in raw_list:
            stocks.append({
                "代码": item['code'],
                "名称": item['name'],
                "最新价": item['last']
            })
        df = pd.DataFrame(stocks)
        return df
    except Exception as e:
        st.error(f"名单拉取异常: {e}")
        return pd.DataFrame()

def fetch_kline_tencent(code):
    """腾讯 K 线穿透接口"""
    # 自动识别沪深前缀
    symbol = f"sh{code}" if code.startswith("60") else f"sz{code}"
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,45,qfq&_={int(time.time())}"
    try:
        r = requests.get(url, timeout=5)
        res = r.json()
        target = res['data'][symbol]
        k_data = target['qfqday'] if 'qfqday' in target else target['day']
        df = pd.DataFrame(k_data, columns=['date', 'open', 'close', 'high', 'low', 'volume'])
        df[['open', 'close']] = df[['open', 'close']].astype(float)
        return df
    except:
        return None

def is_limit_up(close, pre_close):
    if pd.isna(pre_close) or pre_close == 0: return False
    return close >= round(pre_close * 1.10 - 0.01, 2)

def scan_logic(code, name, price):
    try:
        df = fetch_kline_tencent(code)
        if df is None or len(df) < 25: return None
        
        df['pre_close'] = df['close'].shift(1)
        df['is_zt'] = df.apply(lambda x: is_limit_up(x['close'], x['pre_close']), axis=1)
        
        # 严格 13 日判定逻辑 (T-13位是涨停)
        target_idx = len(df) - 14
        if target_idx < 0: return None
        
        if df.loc[target_idx, 'is_zt'] and df.loc[target_idx, 'close'] > df.loc[target_idx, 'open']:
            after_zt = df.loc[target_idx + 1 :, 'is_zt'].sum()
            
            res_type = ""
            if after_zt > 0 and df.loc[target_idx + 1 : target_idx + 10, 'is_zt'].any():
                res_type = "10天双涨停-仅回调13天"
            elif after_zt == 0:
                res_type = "单次涨停-仅回调13天"
            
            # 今日未涨停 (回调状态)
            if res_type and not df.iloc[-1]['is_zt']:
                return {
                    "代码": code, 
                    "名称": name, 
                    "当前价格": price,
                    "强度等级": res_type, 
                    "智能决策": "严格13日：腾讯引擎验证成功",
                    "扫描时间": datetime.now().strftime("%H:%M:%S")
                }
    except: return None
    return None

# --- 3. UI 渲染 ---

if check_password():
    st.title("🚀 游资核心追踪 (腾讯原生-核心版)")
    
    thread_count = st.sidebar.slider("并发扫描强度", 1, 60, 40)
    
    if st.button("开启全量穿透扫描"):
        if 'scan_results' in st.session_state:
            del st.session_state['scan_results']
            
        with st.spinner("📦 正在拉取腾讯全量主板名单..."):
            df_pool = get_tencent_full_pool()
            
            if df_pool.empty:
                st.error("❌ 无法获取名单，请检查网络或刷新页面。")
                st.stop()
            
            # 严格过滤：剔除 ST、创业板(30)、科创板(68)、北交所(8/9)
            df_pool = df_pool[~df_pool['名称'].str.contains("ST|退市")]
            df_pool = df_pool[~df_pool['代码'].str.startswith(("30", "68", "8", "9"))]

        stocks = df_pool.values.tolist()
        st.info(f"📊 名单构建成功：共 {len(stocks)} 只主板标的 | 无换手率限制")
        
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        results = []

        start_time = time.time()
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_stock = {executor.submit(scan_logic, s[0], s[1], s[2]): s for s in stocks}
            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res: 
                    results.append(res)
                    st.toast(f"✅ 捕获: {res['名称']}")
                
                if (i + 1) % 20 == 0 or (i+1) == len(stocks):
                    progress_bar.progress(float((i + 1) / len(stocks)))
                    status_text.text(f"🚀 扫描中: {i+1}/{len(stocks)}")

        total_time = time.time() - start_time
        st.success(f"✨ 扫描结束！耗时 {total_time:.1f} 秒 | 命中 {len(results)} 只标的")
        st.session_state['scan_results'] = results

    if 'scan_results' in st.session_state and st.session_state['scan_results']:
        res_df = pd.DataFrame(st.session_state['scan_results'])
        res_df.insert(0, '序号', range(1, len(res_df) + 1))
        st.divider()
        st.dataframe(
            res_df.style.set_properties(**{'text-align': 'center'}), 
            use_container_width=True, hide_index=True
        )

        output = io.BytesIO()
        res_df.to_excel(output, index=False)
        st.download_button("📥 导出扫描结果", data=output.getvalue(), file_name=f"腾讯选股_{datetime.now().strftime('%m%d')}.xlsx")

    st.divider()
    st.caption("Master Copy | 纯腾讯原生接口 | 取消换手率限制 | 修复0标的问题")
