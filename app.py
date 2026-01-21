import streamlit as st
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
from datetime import datetime, timedelta, timezone
import requests

# --- 1. 配置与安全 (严格遵循 Secrets 模式) ---
st.set_page_config(page_title="游资核心追踪-腾讯极速版", layout="wide")

def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        st.subheader("🛡️ 系统访问安全校验")
        pwd = st.text_input("请输入访问令牌", type="password")
        if st.button("验证并进入系统"):
            # 严格按照 [2026-01-11] 要求，通过 Secrets 读取密码
            target_pwd = st.secrets.get("STOCK_SCAN_PWD")
            if target_pwd and pwd == target_pwd:
                st.session_state["password_correct"] = True
                st.rerun()
            else:
                st.error("令牌校验失败，请检查 Secrets 配置")
        return False
    return True

# --- 2. 腾讯原生 API (无 Akshare，抗波动补丁) ---

@st.cache_data(ttl=600)
def get_stock_pool_stable():
    """获取名单池：增加备用地址防止失联"""
    # 主地址：东财数据 JSON 接口
    url_main = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5000&po=1&np=1&fltt=2&inv=2&fid=f3&fs=m:0+t:6,m:1+t:2&fields=f12,f14,f2,f8"
    # 备用地址：腾讯行情列表接口
    url_backup = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=5000&sort=changepercent&asc=0&node=hs_a&_s_r_a=init"
    
    for url in [url_main, url_backup]:
        try:
            r = requests.get(url, timeout=3)
            if "eastmoney" in url:
                data = r.json()['data']['diff']
                df = pd.DataFrame(data.values())
                df = df[['f12', 'f14', 'f2', 'f8']]
                df.columns = ['代码', '名称', '最新价', '换手率']
                return df
            else:
                # 备用逻辑解析
                data = r.json()
                df = pd.DataFrame(data)[['symbol', 'name', 'trade', 'turnoverratio']]
                df['symbol'] = df['symbol'].str[-6:]
                df.columns = ['代码', '名称', '最新价', '换手率']
                return df
        except:
            continue
    return pd.DataFrame()

def fetch_kline_tencent(code):
    """腾讯原生 K 线接口 (fqkline)"""
    symbol = f"sh{code}" if code.startswith("60") else f"sz{code}"
    # 增加 time.time() 防止缓存导致的旧数据
    url = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline&param={symbol},day,,,45,qfq&_={int(time.time())}"
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
    return close >= round(pre_close * 1.10 - 0.01, 2)

def scan_logic(code, name, price, turnover):
    try:
        df = fetch_kline_tencent(code)
        if df is None or len(df) < 25: return None
        
        df['pre_close'] = df['close'].shift(1)
        df['is_zt'] = df.apply(lambda x: is_limit_up(x['close'], x['pre_close']), axis=1)
        
        # 严格 13 日回调锚点
        target_idx = len(df) - 14
        if target_idx < 0: return None
        
        # 必须是涨停实阳线
        if df.loc[target_idx, 'is_zt'] and df.loc[target_idx, 'close'] > df.loc[target_idx, 'open']:
            after_zt = df.loc[target_idx + 1 :, 'is_zt'].sum()
            
            res_type = ""
            if after_zt > 0 and df.loc[target_idx + 1 : target_idx + 10, 'is_zt'].any():
                res_type = "10天双涨停-仅回调13天"
            elif after_zt == 0:
                res_type = "单次涨停-仅回调13天"
            
            # 今日回调状态判定
            if res_type and not df.iloc[-1]['is_zt']:
                return {
                    "代码": code, "名称": name, "当前价格": price, "换手率": f"{turnover}%",
                    "判定强度": res_type, "智能决策": "严格13日：文字直接展示",
                    "扫描时间": datetime.now().strftime("%H:%M:%S")
                }
    except: return None
    return None

# --- 3. UI 渲染与执行 ---

if check_password():
    st.title("🚀 游资核心追踪 (腾讯原生全流程版)")
    
    # 侧边栏配置
    thread_count = st.sidebar.slider("并发扫描强度", 1, 50, 30)
    
    if st.button("开启 13 日周期穿透扫描"):
        if 'scan_results' in st.session_state:
            del st.session_state['scan_results']
            
        with st.spinner("📦 正在极速同步名单池 (跨节点容错机制)..."):
            df_pool = get_stock_pool_stable()
            if df_pool.empty:
                st.error("⚠️ 接口连接繁忙：请等待 15 秒后点击刷新页面。")
                st.stop()
            
            # 剔除 ST/创业板/科创板/换手率<3 (母版核心)
            df_pool = df_pool[~df_pool['名称'].str.contains("ST|退市")]
            df_pool = df_pool[~df_pool['代码'].str.startswith(("30", "68", "9"))]
            df_pool = df_pool[df_pool['换手率'].astype(float) >= 3.0]

        stocks = df_pool.values.tolist()
        st.info(f"📊 待判定标的：{len(stocks)} 只 | 接口状态：已连接")
        
        progress_bar = st.progress(0.0)
        results = []

        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            future_to_stock = {executor.submit(scan_logic, s[0], s[1], s[2], s[3]): s for s in stocks}
            for i, future in enumerate(as_completed(future_to_stock)):
                res = future.result()
                if res: 
                    results.append(res)
                    st.toast(f"✅ 捕获: {res['名称']}")
                
                if (i + 1) % 10 == 0 or (i+1) == len(stocks):
                    progress_bar.progress(float((i + 1) / len(stocks)))

        st.success(f"✨ 扫描完成！本次命中符合 13 日回调逻辑的个股：{len(results)} 只")
        st.session_state['scan_results'] = results

    # 4. 结果展示 (序号居中)
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
        st.download_button("📥 导出全量结果", data=output.getvalue(), file_name=f"13日回调_{datetime.now().strftime('%m%d')}.xlsx")

    st.divider()
    st.caption("Master Copy | 纯腾讯原生接口 | 序号居中稳定版 | 严格仅限13天回调")
