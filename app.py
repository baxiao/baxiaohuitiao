import streamlit as st
import pandas as pd
import akshare as ak
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ==========================================
# 1. 基础配置与安全（模仿DeepSeek API Key模式）
# ==========================================
# 可以在系统环境变量中设置 ACCESS_PASSWORD，或在这里修改默认值
# 提示：请不要直接在代码中明文存储生产环境密码
SYS_PASSWORD = os.getenv("STOCK_SCAN_PWD", "wen666") 

# ==========================================
# 2. 核心选股逻辑类
# ==========================================
class StockStrategy:
    def __init__(self):
        self.results = []
        self.lock = threading.Lock()

    def is_limit_up(self, close, pre_close):
        """主板涨停判断"""
        return close >= round(pre_close * 1.10 - 0.01, 2)

    def analyze(self, code, name):
        try:
            # 获取最近30个交易日数据
            df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq").tail(30)
            if len(df) < 25: return
            
            # 预处理数据
            df = df.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low', '成交量': 'vol'})
            df['pre_close'] = df['close'].shift(1)
            df['is_zt'] = df.apply(lambda x: self.is_limit_up(x['close'], x['pre_close']), axis=1)
            
            # --- 关键：定位13天前的索引 ---
            # 今天是 -1，昨天是 -2 ... 第13天回调结束（即第14天前涨停）
            target_idx = -14 
            
            if df['is_zt'].iloc[target_idx]:
                # 检查之后13天内的涨停情况
                after_zt_slice = df['is_zt'].iloc[target_idx + 1:]
                zt_count_after = after_zt_slice.sum()
                
                # 功能2：单次涨停隔日起回调13天
                if zt_count_after == 0:
                    self.add_result(code, name, "单次涨停回调13天")
                
                # 功能1：10天内双涨停，首根后回调13天
                else:
                    # 寻找第二根涨停的位置
                    # 检查从首根涨停后的10天内是否有第二根
                    ten_day_slice = df['is_zt'].iloc[target_idx + 1 : target_idx + 11]
                    if ten_day_slice.any():
                        self.add_result(code, name, "10天双停回调13天")
                        
        except:
            pass

    def add_result(self, code, name, strategy_type):
        with self.lock:
            self.results.append({
                "代码": code,
                "名称": name,
                "策略类型": strategy_type,
                "当前日期": datetime.datetime.now().strftime('%Y-%m-%d')
            })

# ==========================================
# 3. 网页前端界面 (Streamlit)
# ==========================================
def main():
    st.set_page_config(page_title="文哥哥专用选股系统", layout="wide")
    st.title("🚀 13日回调选股系统 (2026版)")

    # 密码访问模块
    with st.sidebar:
        st.header("访问控制")
        input_pwd = st.text_input("请输入访问密码", type="password")
        if input_pwd != SYS_PASSWORD:
            st.warning("密码不正确，功能已锁定。")
            return
        st.success("认证通过")
        st.divider()
        scan_btn = st.button("开始全市场扫描")

    if scan_btn:
        scanner = StockStrategy()
        
        # 获取全量股票
        with st.spinner("正在获取全市场列表并过滤..."):
            all_stocks = ak.stock_info_a_code_name()
            # 剔除ST、创业板(30)、科创板(68)
            filtered_stocks = all_stocks[
                (~all_stocks['name'].str.contains('ST')) & 
                (~all_stocks['code'].str.startswith(('30', '68')))
            ]
            stock_list = filtered_stocks.values.tolist()

        # 多线程扫描
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with ThreadPoolExecutor(max_workers=30) as executor:
            future_to_stock = {executor.submit(scanner.analyze, s[0], s[1]): s for s in stock_list}
            completed = 0
            for future in as_completed(future_to_stock):
                completed += 1
                if completed % 50 == 0:
                    progress = completed / len(stock_list)
                    progress_bar.progress(progress)
                    status_text.text(f"已扫描 {completed}/{len(stock_list)} 只股票...")

        # 结果展示
        if scanner.results:
            df_final = pd.DataFrame(scanner.results)
            # 序号居中处理
            df_final.insert(0, '序号', range(1, len(df_final) + 1))
            
            st.subheader(f"✅ 扫描完成，共找到 {len(df_final)} 只目标股")
            
            # 表格显示
            st.dataframe(df_final.style.set_properties(**{'text-align': 'center'}), use_container_width=True)

            # Excel 导出
            file_name = f"选股结果_{datetime.datetime.now().strftime('%H%M%S')}.xlsx"
            df_final.to_excel(file_name, index=False)
            with open(file_name, "rb") as f:
                st.download_button("📥 导出 Excel 结果", f, file_name=file_name)
        else:
            st.info("今日未扫描到符合条件的个股。")

if __name__ == "__main__":
    main()
