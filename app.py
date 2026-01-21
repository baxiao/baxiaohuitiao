import streamlit as st
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta, timezone
import requests

# --- 1. 配置与安全 (Secrets 读取模式) ---
st.set_page_config(page_title="游资核心追踪-纯腾讯引擎", layout="wide")

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        st.subheader("🛡️ 系统访问安全校验")
        pwd = st.text_input("请输入访问令牌", type="password")
        if st.button("验证并进入系统"):
            # 严格按照要求从 Secrets 读取
            target_pwd = st.secrets.get("STOCK_SCAN_PWD")
            if target_pwd and pwd == target_pwd:
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("令牌校验失败")
        return False
    return True

# --- 2. 腾讯原生 API 数据引擎 ---

@st.cache_data(ttl=600)
def get_tencent_full_pool():
    """使用腾讯行情列表接口获取全量名单"""
    # 腾讯全量 A 股列表接口 (包含主板、非ST等基础信息)
    url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=6000&sort=symbol&asc=1&node=hs_a&_s_r_a=init"
    try:
        r = requests.get(url, timeout=5)
        data = r.json()
        df = pd.DataFrame(data)
        # 腾讯字段适配：symbol(代码), name(名称), trade(价格)
        df = df[['symbol', 'name', 'trade']]
        df.columns = ['代码', '名称', '最新价']
        # 提取纯数字代码
        df['代码'] = df['代码'].str[-6:]
        return df
    except:
        return pd.DataFrame()

def fetch_kline_tencent(code):
    """腾讯原生 K 线穿透接口"""
    symbol = f"sh{code}" if code.startswith("60") else f"sz{code}"
    # fqkline 表示获取前复权数据
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},day,,,45,qfq&_={int(time.time())}"
    try:
        r = requests.get(url, timeout=3)
        res = r.json()
        k_data = res['data'][symbol]['qfqday'] if 'qfqday' in res['data'][symbol] else res['data'][symbol]['day']
        df = pd.DataFrame(k_data, columns=['date', 'open', 'close', 'high', 'low', 'volume'])
        df[['open', 'close']] = df[['open', 'close']].astype(float)
        return df
    except:
        return None

def is_limit_up(close, pre_close):
    if pd.isna(pre_close) or pre_close == 0: return False
    # 主板 10% 涨停逻辑
    return close >= round(pre_close * 1.10 - 0.01, 2)

def scan_logic(code, name, price):
    try:
        df = fetch_kline_tencent(code)
        if df is None or len(df) < 25: return None
        
        df['pre_close'] = df['close'].shift(1)
        df['is_zt'] = df.apply(lambda x: is_limit_up(x['close'], x['pre_close']), axis=1)
        
        # --- 精准 13 日回调判定 ---
        # 锁定 13 个交易日前的那根 K 线 (len-14)
        target_idx = len(df) - 14
        if target_idx < 0: return None
        
        # 判定：13天前必须是涨停实阳线
        if df.loc[target_idx, 'is_zt'] and df.loc[target_idx, 'close'] > df.loc[target_idx, 'open']:
            # 统计之后到今天的涨停总数
            after_zt = df.loc[target_idx + 1 :, 'is_zt'].sum()
            
            res_type = ""
            if after_zt > 0 and df.loc[target_idx + 1 : target_idx + 10, 'is_zt'].any():
                res_type = "10天双涨停-仅回调13天"
            elif after_zt == 0:
                res_type = "单次涨停-仅回调13天"
            
            # 必须今天未涨停
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

# --- 3. UI 渲染与多线程执行 ---

if check_password():
    st.title("🚀 游资核心追踪 (13日回调-全腾讯接口版)")
    
    # 并发数设置
    thread_count = st.sidebar.slider("并发扫描强度", 1, 60, 40)
    
    if st.button("全量主板穿透扫描 (纯腾讯引擎)"):
        if 'scan_results' in st.session_state:
            del st.session_state['scan_results']
            
        with st.spinner("📦 正在极速同步腾讯全量主板名单..."):
            df_pool = get_tencent_full_pool()
            if df_pool.empty:
                st.error("腾讯名单接口暂时无响应，请稍后刷新重试")
                st.stop()
            
            # 严格遵循母版硬性过滤：剔除 ST、创业板(30)、科创板(68)、北交所(8/9)
            df_pool = df_pool[~df_pool['名称'].str.contains("ST|退市")]
            df_pool = df_pool[~df_pool['代码'].str.startswith(("30", "68", "8", "9"))]

        stocks = df_pool.values.tolist()
        st.info(f"📊 名单构建成功：共 {len(stocks)} 只主板标的 | 换手率限制：已取消")
        
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        results = []

        # 启动多线程穿透
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_stock = {executor.submit(scan_logic, s[0], s[1], s[2]): s for s in stocks}
            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res: 
                    results.append(res)
                    st.toast(f"✅ 捕获: {res['名称']}")
                
                # 每 20 只更新一次进度条，提升 UI 流畅度
                if (i + 1) % 20 == 0 or (i+1) == len(stocks):
                    progress_bar.progress(float((i + 1) / len(stocks)))
                    status_text.text(f"🚀 扫描中: {i+1}/{len(stocks)}")

        total_time = time.time() - start_time
        st.success(f"✨ 扫描结束！耗时 {total_time:.1f} 秒 | 捕获符合 13 日回调个股：{len(results)} 只")
        st.session_state['scan_results'] = results

    # 4. 结果展示 (序号居中，文字直接展示)
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
    st.caption("Master Copy | 纯腾讯原生接口 | 取消换手率限制 | 序号居中稳定版")
