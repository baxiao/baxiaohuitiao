import streamlit as st
import pandas as pd
import akshare as ak
import datetime
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ==========================================
# 1. 基础配置与安全 (仅从 Secrets 读取)
# ==========================================
if "STOCK_SCAN_PWD" in st.secrets:
    SYS_PASSWORD = st.secrets["STOCK_SCAN_PWD"]
else:
    st.error("❌ 系统配置错误：请在 Streamlit 控制台的 Secrets 中设置 'STOCK_SCAN_PWD'。")
    st.stop()  # 停止后续代码运行

# ==========================================
# 2. 核心选股逻辑类
# ==========================================
class StockStrategy:
    def __init__(self):
        self.results = []
        self.lock = threading.Lock()

    def is_limit_up(self, close, pre_close):
        """主板涨停判断：10%"""
        if pd.isna(pre_close) or pre_close == 0: return False
        # 兼容主板 10% 涨停精度
        return close >= round(pre_close * 1.10 - 0.01, 2)

    def analyze(self, code, name):
        try:
            # 获取最近30个交易日数据
            df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq").tail(30)
            if len(df) < 25: return
            
            df = df.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'vol'})
            df['pre_close'] = df['close'].shift(1)
            df['is_zt'] = df.apply(lambda x: self.is_limit_up(x['close'], x['pre_close']), axis=1)
            
            # 定位 13 个交易日前（Python 索引 -14）
            # 逻辑：第1天涨停，第2-14天(共13天)回调，今天刚好是回调第13天
            target_idx = -14 
            
            if df['is_zt'].iloc[target_idx]:
                # 检查之后 13 天内的涨停数量
                after_zt_slice = df['is_zt'].iloc[target_idx + 1:]
                zt_count_after = after_zt_slice.sum()
                
                # 功能 2：单次涨停隔日起回调 13 天 (后续 13 天内无涨停)
                if zt_count_after == 0:
                    self.add_result(code, name, "单次涨停回调13天")
                
                # 功能 1：10天内双涨停，首根后回调 13 天
                else:
                    # 检查首根后的 10 天内（包括首根后的第1天到第10天）是否有第二根涨停
                    ten_day_slice = df['is_zt'].iloc[target_idx + 1 : target_idx + 11]
                    if ten_day_slice.any():
                        self.add_result(code, name, "10天双停回调13天")
        except Exception:
            pass

    def add_result(self, code, name, strategy_type):
        with self.lock:
            self.results.append({
                "代码": code,
                "名称": name,
                "策略类型": strategy_type,
                "触发日期": datetime.datetime.now().strftime('%Y-%m-%d')
            })

# ==========================================
# 3. 网页前端界面
# ==========================================
def main():
    st.set_page_config(page_title="文哥哥选股系统", layout="wide")
    st.title("📈 13日回调选股系统 (2026版)")

    # 侧边栏登录
    with st.sidebar:
        st.header("安全验证")
        input_pwd = st.text_input("输入访问密码", type="password")
        if not input_pwd:
            st.info("请输入密码解锁功能")
            return
        if input_pwd != SYS_PASSWORD:
            st.error("🔒 密码错误，请重新输入")
            return
        st.success("✅ 认证通过")
        st.divider()
        scan_btn = st.button("🚀 开始全市场扫描")

    if scan_btn:
        scanner = StockStrategy()
        
        # --- 获取股票列表 (优化后的接口与重试) ---
        with st.spinner("正在安全连接行情接口..."):
            all_stocks = None
            for i in range(3): # 失败重试 3 次
                try:
                    all_stocks = ak.stock_zh_a_spot_em() 
                    if all_stocks is not None: break
                except:
                    time.sleep(2)
            
            if all_stocks is None:
                st.error("数据接口请求过于频繁，请 1 分钟后再点击扫描。")
                return

            # 板块过滤：剔除 ST、创业板、科创板 (符合母版要求)
            filtered = all_stocks[
                (~all_stocks['名称'].str.contains('ST')) & 
                (~all_stocks['代码'].str.startswith(('30', '68')))
            ].copy()
            stock_list = filtered[['代码', '名称']].values.tolist()

        # --- 多线程扫描 (母版多线程架构) ---
        progress_bar = st.progress(0)
        status_msg = st.empty()
        
        # Streamlit Cloud 环境设为 25 线程以防被接口封禁
        with ThreadPoolExecutor(max_workers=25) as executor:
            future_to_stock = {executor.submit(scanner.analyze, s[0], s[1]): s for s in stock_list}
            completed = 0
            total = len(stock_list)
            
            for future in as_completed(future_to_stock):
                completed += 1
                if completed % 100 == 0:
                    progress_bar.progress(completed / total)
                    status_msg.text(f"已扫描 {completed}/{total} 只个股...")

        # --- 结果展示 (序号居中稳定版) ---
        if scanner.results:
            df_res = pd.DataFrame(scanner.results)
            df_res.insert(0, '序号', range(1, len(df_res) + 1))
            
            st.subheader(f"🎯 扫描完成：符合条件个股 ({len(df_res)} 只)")
            
            # 渲染表格：序号和文字居中
            st.dataframe(
                df_res.style.set_properties(**{'text-align': 'center'}).set_table_styles([dict(selector='th', props=[('text-align', 'center')])]), 
                use_container_width=True
            )

            # Excel 导出
            excel_name = f"callback_13d_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
            df_res.to_excel(excel_name, index=False)
            with open(excel_name, "rb") as f:
                st.download_button("📥 导出扫描结果 (Excel)", f, file_name=excel_name)
        else:
            st.info("今日扫描结束，未发现符合形态的个股。")

if __name__ == "__main__":
    main()
