import streamlit as st
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime
import requests
import random

# --- 1. 配置与安全 ---
st.set_page_config(page_title="游资核心追踪-最终稳定版", layout="wide")

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

# --- 2. 数据引擎 (新浪名单 + 腾讯K线) ---

@st.cache_data(ttl=3600)
def get_sina_stock_pool():
    """从新浪财经拉取全量名单，这个接口在海外访问极稳"""
    url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=6000&sort=symbol&asc=1&node=hs_a&_s_r_a=init"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        r = requests.get(url, headers=headers, timeout=10)
        # 新浪返回的不是标准JSON格式(键名没引号)，需要特殊处理
        import ast
        raw_data = r.text
        # 将 JS 对象格式转为 Python 列表
        data = ast.literal_eval(raw_data)
        
        df = pd.DataFrame(data)
        # 腾讯字段：symbol, name, trade
        df = df[['symbol', 'name', 'trade']]
        df.columns = ['代码', '名称', '最新价']
        # 统一代码格式，去掉 sh/sz 前缀
        df['代码'] = df['代码'].str[-6:]
        return df
    except Exception as e:
        st.error(f"名单拉取异常: 新浪接口连接超时，请重试。")
        return pd.DataFrame()

def fetch_kline_tencent(code):
    """腾讯 K 线穿透接口 (判定逻辑不变)"""
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
        
        # 严格 13 日回调锚点 (T-13是涨停)
        target_idx = len(df) - 14
        if target_idx < 0: return None
        
        # 必须是涨停阳线
        if df.loc[target_idx, 'is_zt'] and df.loc[target_idx, 'close'] > df.loc[target_idx, 'open']:
            after_zt = df.loc[target_idx + 1 :, 'is_zt'].sum()
            
            res_type = ""
            if after_zt > 0 and df.loc[target_idx + 1 : target_idx + 10, 'is_zt'].any():
                res_type = "10天双涨停-仅回调13天"
            elif after_zt == 0:
                res_type = "单次涨停-仅回调13天"
            
            # 今日非涨停
            if res_type and not df.iloc[-1]['is_zt']:
                return {
                    "代码": code, "名称": name, "当前价格": price,
                    "强度等级": res_type, "智能决策": "严格13日：穿透验证成功",
                    "扫描时间": datetime.now().strftime("%H:%M:%S")
                }
    except: return None
    return None

# --- 3. UI 渲染 ---

if check_password():
    st.title("🚀 游资核心追踪 (终极稳定-全量扫描版)")
    
    thread_count = st.sidebar.slider("并发扫描强度", 1, 60, 40)
    
    if st.button("开始全量 13 日周期穿透"):
        if 'scan_results' in st.session_state:
            del st.session_state['scan_results']
            
        with st.spinner("📦 正在极速拉取 A 股全量名单..."):
            df_pool = get_sina_stock_pool()
            
            if df_pool.empty:
                st.stop()
            
            # 过滤：剔除 ST、创业板(30)、科创板(68)、北交所(8/9)
            df_pool = df_pool[~df_pool['名称'].str.contains("ST|退市")]
            df_pool = df_pool[~df_pool['代码'].str.startswith(("30", "68", "8", "9"))]

        stocks = df_pool.values.tolist()
        st.info(f"📊 名单拉取成功：共 {len(stocks)} 只主板标的 | 无换手率限制")
        
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
                    status_text.text(f"🚀 扫描进度: {i+1}/{len(stocks)}")

        total_time = time.time() - start_time
        st.success(f"✨ 扫描结束！耗时 {total_time:.1f} 秒 | 捕获符合 13 日回调逻辑个股：{len(results)} 只")
        st.session_state['scan_results'] = results

    # 展示结果
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
        st.download_button("📥 导出全量结果 (Excel)", data=output.getvalue(), file_name=f"13日扫描_{datetime.now().strftime('%m%d')}.xlsx")

    st.divider()
    st.caption("Master Copy | 序号居中 | 新浪名单+腾讯K线 | 严格 13 日回调判定")
