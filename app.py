import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
import threading
import concurrent.futures
import requests
import io
from typing import List, Dict, Tuple, Optional

# ========== 全局配置 ==========
st.set_page_config(
    page_title="股票涨停回调筛选系统",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 强制指定东方财富数据源，提高稳定性
ak.set_option("akshare.data_source", "eastmoney")

# 全局变量
lock = threading.Lock()
results_cache = {}
filtering_status = {"running": False, "progress": 0, "message": "等待开始"}

# ========== 认证功能 ==========
def authenticate_user() -> bool:
    """用户认证 - 仅密码验证，从Secrets获取"""
    if 'authenticated' in st.session_state and st.session_state.authenticated:
        return True
    
    st.title("🔒 股票涨停回调筛选系统 - 登录")
    expected_password = st.secrets.get("app_password", "stock123456")
    
    with st.form("login_form"):
        password = st.text_input("请输入访问密码", type="password")
        submit_button = st.form_submit_button("登录")
        
        if submit_button:
            if password == expected_password:
                st.session_state.authenticated = True
                st.success("登录成功！即将进入系统...")
                time.sleep(1)
                st.rerun()
            else:
                st.error("密码错误，请重试")
    
    return False

# ========== 数据获取核心函数 ==========
def get_stock_list() -> pd.DataFrame:
    """获取A股股票列表 - 多数据源+重试机制，提高稳定性"""
    # 定义备选数据源函数列表
    data_sources = [
        lambda: ak.stock_info_a_code_name(),  # 数据源1：东方财富全市场代码
        lambda: ak.stock_zh_a_spot_em()[['代码', '名称']]  # 数据源2：同花顺实时行情表
    ]
    
    # 重试3次，遍历所有数据源
    for retry in range(3):
        for source_func in data_sources:
            try:
                df = source_func()
                if not df.empty and '代码' in df.columns and '名称' in df.columns:
                    st.success(f"成功获取股票列表，共{len(df)}只股票")
                    return df.drop_duplicates(subset=['代码'])
            except Exception as e:
                st.warning(f"数据源尝试失败: {str(e)}，正在重试...")
                time.sleep(1)  # 重试间隔1秒
    
    st.error("获取股票列表失败：所有数据源连接异常，请检查网络或稍后重试")
    return pd.DataFrame(columns=['代码', '名称'])

def get_stock_sector(stock_code: str) -> str:
    """获取股票所属板块 - 带超时处理"""
    try:
        # 设置请求超时，避免卡死
        session = requests.Session()
        session.timeout = 5
        ak.session = session
        
        sector_df = ak.stock_sector_spot_em(symbol=stock_code)
        return sector_df.iloc[0]['行业板块'] if not sector_df.empty else "未知"
    except:
        return "未知"

def get_stock_data(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取股票历史行情数据 - 带超时和异常处理"""
    try:
        # 设置请求超时
        session = requests.Session()
        session.timeout = 10
        ak.session = session
        
        # 获取前复权数据
        stock_df = ak.stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )
        return stock_df if not stock_df.empty else pd.DataFrame()
    except Exception as e:
        st.warning(f"[{stock_code}] 数据获取失败: {str(e)}")
        return pd.DataFrame()

# ========== 涨停判断与模式筛选 ==========
def is_limit_up(close_price: float, pre_close: float) -> bool:
    """判断是否涨停：普通股≥9.8%，ST股≥4.8%"""
    if pre_close == 0:
        return False
    change_pct = (close_price - pre_close) / pre_close * 100
    # 兼容ST股和普通股票的不同涨跌幅限制
    return change_pct >= 9.8 or (change_pct >= 4.8 and "ST" in str(close_price))

def find_limit_up_days(stock_df: pd.DataFrame) -> List[int]:
    """找出股票所有涨停日期的索引"""
    limit_up_indices = []
    for i in range(1, len(stock_df)):
        if is_limit_up(stock_df.iloc[i]['收盘'], stock_df.iloc[i-1]['收盘']):
            limit_up_indices.append(i)
    return limit_up_indices

def check_double_limit_up_pattern(stock_df: pd.DataFrame, limit_up_indices: List[int]) -> List[Dict]:
    """功能1：筛选10天内双涨停，且第二次涨停后回调13天内的股票"""
    results = []
    for i in range(len(limit_up_indices)-1):
        first_idx = limit_up_indices[i]
        second_idx = limit_up_indices[i+1]
        
        # 两个涨停间隔≤10天
        if second_idx - first_idx <= 10:
            # 第二次涨停后有至少13天的回调期
            if len(stock_df) - second_idx >= 13:
                pullback_df = stock_df.iloc[second_idx:second_idx+13]
                max_pullback = (pullback_df.iloc[0]['收盘'] - pullback_df['收盘'].min()) / pullback_df.iloc[0]['收盘'] * 100
                
                results.append({
                    'pattern_type': '双涨停回调',
                    'first_limit_up_date': stock_df.iloc[first_idx]['日期'],
                    'second_limit_up_date': stock_df.iloc[second_idx]['日期'],
                    'pullback_days': 13,
                    'max_pullback_pct': round(max_pullback, 2),
                    'latest_price': round(pullback_df.iloc[-1]['收盘'], 2),
                    'status': '符合条件'
                })
    return results

def check_single_limit_up_pattern(stock_df: pd.DataFrame, limit_up_indices: List[int]) -> List[Dict]:
    """功能2：筛选单次涨停，隔日起回调13天内的股票"""
    results = []
    for idx in limit_up_indices:
        # 涨停次日起有至少13天的回调期
        if len(stock_df) - idx >= 14:
            pullback_df = stock_df.iloc[idx+1:idx+14]
            max_pullback = (stock_df.iloc[idx]['收盘'] - pullback_df['收盘'].min()) / stock_df.iloc[idx]['收盘'] * 100
            
            results.append({
                'pattern_type': '单涨停回调',
                'limit_up_date': stock_df.iloc[idx]['日期'],
                'pullback_start_date': pullback_df.iloc[0]['日期'],
                'pullback_days': 13,
                'max_pullback_pct': round(max_pullback, 2),
                'latest_price': round(pullback_df.iloc[-1]['收盘'], 2),
                'status': '符合条件'
            })
    return results

# ========== 多线程处理 ==========
def process_single_stock(stock_code: str, stock_name: str, start_date: str, end_date: str, sector_filter: str = None) -> List[Dict]:
    """单只股票处理函数 - 供多线程调用"""
    global filtering_status
    
    try:
        # 板块过滤
        if sector_filter and sector_filter != "全部":
            sector = get_stock_sector(stock_code)
            if sector != sector_filter:
                return []
        
        # 获取股票数据
        stock_df = get_stock_data(stock_code, start_date, end_date)
        if stock_df.empty:
            return []
        
        # 查找涨停日期并筛选模式
        limit_up_indices = find_limit_up_days(stock_df)
        if not limit_up_indices:
            return []
        
        double_results = check_double_limit_up_pattern(stock_df, limit_up_indices)
        single_results = check_single_limit_up_pattern(stock_df, limit_up_indices)
        all_results = double_results + single_results
        
        # 添加股票基本信息
        for res in all_results:
            res['stock_code'] = stock_code
            res['stock_name'] = stock_name
            res['sector'] = get_stock_sector(stock_code)
        
        # 更新进度
        with lock:
            filtering_status["progress"] += 1
        
        return all_results
    except Exception as e:
        with lock:
            filtering_status["progress"] += 1
        return []

def filter_stocks_multithread(stock_list: pd.DataFrame, start_date: str, end_date: str, sector_filter: str = None) -> List[Dict]:
    """多线程批量筛选股票"""
    global filtering_status
    filtering_status = {"running": True, "progress": 0, "message": "开始筛选..."}
    
    all_results = []
    total_stocks = len(stock_list)
    if total_stocks == 0:
        return all_results
    
    # 多线程处理，线程数适配云环境
    max_workers = min(5, total_stocks)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for _, row in stock_list.iterrows():
            futures.append(executor.submit(
                process_single_stock,
                row['代码'], row['名称'],
                start_date, end_date,
                sector_filter
            ))
        
        # 实时更新进度
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for future in concurrent.futures.as_completed(futures):
            try:
                res = future.result()
                if res:
                    all_results.extend(res)
            except Exception as e:
                pass
            
            # 更新进度显示
            progress = int(filtering_status["progress"] / total_stocks * 100)
            progress_bar.progress(progress)
            status_text.text(f"筛选进度: {progress}% ({filtering_status['progress']}/{total_stocks})")
    
    filtering_status["running"] = False
    filtering_status["message"] = "筛选完成！"
    progress_bar.progress(100)
    status_text.text("筛选完成！")
    return all_results

# ========== Excel导出 ==========
def export_to_excel(results: List[Dict]) -> bytes:
    """筛选结果导出为Excel文件"""
    if not results:
        return None
    
    df = pd.DataFrame(results)
    # 列顺序整理
    columns_order = [
        'stock_code', 'stock_name', 'sector', 'pattern_type',
        'first_limit_up_date', 'second_limit_up_date', 'limit_up_date',
        'pullback_start_date', 'pullback_days', 'max_pullback_pct',
        'latest_price', 'status'
    ]
    # 补全缺失列
    for col in columns_order:
        if col not in df.columns:
            df[col] = ""
    
    df = df[columns_order].rename(columns={
        'stock_code': '股票代码', 'stock_name': '股票名称', 'sector': '所属板块',
        'pattern_type': '模式类型', 'first_limit_up_date': '第一次涨停日期',
        'second_limit_up_date': '第二次涨停日期', 'limit_up_date': '涨停日期',
        'pullback_start_date': '回调开始日期', 'pullback_days': '回调天数',
        'max_pullback_pct': '最大回调幅度(%)', 'latest_price': '最新价格', 'status': '状态'
    })
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='涨停回调筛选结果')
    output.seek(0)
    return output.getvalue()

# ========== 主界面 ==========
def main():
    if not authenticate_user():
        return
    
    st.title("📈 股票涨停回调筛选系统")
    
    # 侧边栏配置
    with st.sidebar:
        st.header("🔧 筛选配置")
        # 日期选择
        default_start = datetime.datetime.now() - datetime.timedelta(days=60)
        start_date = st.date_input("筛选开始日期", value=default_start)
        end_date = st.date_input("筛选结束日期", value=datetime.datetime.now())
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        # 板块筛选
        sector_options = ["全部", "金融", "医药生物", "电子", "计算机", "机械设备", "国防军工", "消费"]
        selected_sector = st.selectbox("行业板块筛选", sector_options)
        
        # 功能按钮
        filter_btn = st.button("🚀 开始筛选", type="primary")
        clear_btn = st.button("🧹 清除缓存")
        
        if clear_btn:
            results_cache.clear()
            st.success("缓存已清除！")
    
    # 主内容区
    if filter_btn:
        with st.spinner("正在获取股票列表..."):
            stock_list = get_stock_list()
        
        if not stock_list.empty:
            st.info(f"📋 筛选条件：{start_date_str} 至 {end_date_str} | 板块：{selected_sector}")
            results = filter_stocks_multithread(
                stock_list, start_date_str, end_date_str,
                selected_sector if selected_sector != "全部" else None
            )
            
            # 缓存结果
            results_cache['last_results'] = results
            results_cache['params'] = (start_date_str, end_date_str, selected_sector)
            
            # 显示结果
            if results:
                st.success(f"✅ 共筛选出 {len(results)} 只符合条件的股票")
                st.dataframe(pd.DataFrame(results), use_container_width=True)
                
                # 导出按钮
                excel_data = export_to_excel(results)
                st.download_button(
                    label="📥 导出Excel结果",
                    data=excel_data,
                    file_name=f"涨停回调筛选结果_{start_date_str}_{end_date_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            else:
                st.info("📭 未找到符合条件的股票")
    
    # 显示历史缓存结果
    if 'last_results' in results_cache and results_cache['last_results']:
        st.subheader("📊 历史筛选结果")
        st.info(f"上次筛选条件：{results_cache['params'][0]} 至 {results_cache['params'][1]} | 板块：{results_cache['params'][2]}")
        st.dataframe(pd.DataFrame(results_cache['last_results']), use_container_width=True)

if __name__ == "__main__":
    main()
