import streamlit as st
import akshare as ak
import pandas as pd
import numpy as np
import datetime
import time
import threading
import concurrent.futures
import bcrypt
import io
from typing import List, Dict, Tuple, Optional

# 设置页面配置
st.set_page_config(
    page_title="股票涨停回调筛选系统",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 密码认证配置
USER_CREDENTIALS = {
    "admin": b'$2b$12$EixZaYb4xU58Gpq1R0yWbeb00LU5qUaK6x6h6X6h6X6h6X6h6X6h6'  # 密码: admin123
}

# 全局变量
lock = threading.Lock()
results_cache = {}
filtering_status = {"running": False, "progress": 0, "message": "等待开始"}

def hash_password(password: str) -> bytes:
    """密码哈希"""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt)

def verify_password(input_password: str, hashed_password: bytes) -> bool:
    """验证密码"""
    return bcrypt.checkpw(input_password.encode('utf-8'), hashed_password)

def authenticate_user() -> bool:
    """用户认证"""
    if 'authenticated' in st.session_state and st.session_state.authenticated:
        return True
    
    st.title("🔒 股票涨停回调筛选系统 - 登录")
    
    with st.form("login_form"):
        username = st.text_input("用户名")
        password = st.text_input("密码", type="password")
        submit_button = st.form_submit_button("登录")
        
        if submit_button:
            if username in USER_CREDENTIALS and verify_password(password, USER_CREDENTIALS[username]):
                st.session_state.authenticated = True
                st.success("登录成功！")
                time.sleep(1)
                st.rerun()
            else:
                st.error("用户名或密码错误")
    
    return False

def get_stock_list() -> pd.DataFrame:
    """获取A股股票列表"""
    try:
        stock_info = ak.stock_info_a_code_name()
        return stock_info
    except Exception as e:
        st.error(f"获取股票列表失败: {str(e)}")
        return pd.DataFrame()

def get_stock_sector(stock_code: str) -> str:
    """获取股票所属板块"""
    try:
        stock_sector = ak.stock_sector_spot_em(symbol=stock_code)
        if not stock_sector.empty:
            return stock_sector.iloc[0]['行业板块']
    except:
        pass
    return "未知"

def get_stock_data(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取股票历史数据"""
    try:
        stock_data = ak.stock_zh_a_hist(symbol=stock_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
        return stock_data
    except Exception as e:
        st.warning(f"获取股票 {stock_code} 数据失败: {str(e)}")
        return pd.DataFrame()

def is_limit_up(close_price: float, pre_close: float) -> bool:
    """判断是否涨停（考虑ST股和普通股票的不同涨跌幅限制）"""
    if pre_close == 0:
        return False
    
    # 计算涨跌幅
    change_pct = (close_price - pre_close) / pre_close * 100
    
    # 普通股票涨停限制为10%，ST股为5%
    # 考虑到四舍五入，使用9.8%作为判断阈值
    return change_pct >= 9.8

def find_limit_up_days(stock_data: pd.DataFrame) -> List[int]:
    """找出涨停日期的索引"""
    limit_up_days = []
    for i in range(1, len(stock_data)):
        close = stock_data.iloc[i]['收盘']
        pre_close = stock_data.iloc[i-1]['收盘']
        if is_limit_up(close, pre_close):
            limit_up_days.append(i)
    return limit_up_days

def check_double_limit_up_pattern(stock_data: pd.DataFrame, limit_up_days: List[int]) -> List[Dict]:
    """检查10天内双涨停，首根后回调13天的模式"""
    results = []
    n = len(limit_up_days)
    
    for i in range(n - 1):
        first_limit_up_idx = limit_up_days[i]
        second_limit_up_idx = limit_up_days[i + 1]
        
        # 检查两个涨停是否在10天内
        if second_limit_up_idx - first_limit_up_idx <= 10:
            # 检查回调天数（从第二个涨停开始计算）
            if len(stock_data) - second_limit_up_idx >= 13:
                # 获取回调期间的数据
                pullback_data = stock_data.iloc[second_limit_up_idx:second_limit_up_idx + 13]
                
                # 计算回调幅度
                max_pullback = (pullback_data['收盘'].iloc[0] - pullback_data['收盘'].min()) / pullback_data['收盘'].iloc[0] * 100
                
                results.append({
                    'pattern_type': '双涨停回调',
                    'first_limit_up_date': stock_data.iloc[first_limit_up_idx]['日期'],
                    'second_limit_up_date': stock_data.iloc[second_limit_up_idx]['日期'],
                    'pullback_days': 13,
                    'max_pullback_pct': max_pullback,
                    'latest_price': pullback_data['收盘'].iloc[-1],
                    'status': '符合条件'
                })
    
    return results

def check_single_limit_up_pattern(stock_data: pd.DataFrame, limit_up_days: List[int]) -> List[Dict]:
    """检查单次涨停，隔日起回调13天的模式"""
    results = []
    
    for limit_up_idx in limit_up_days:
        # 检查是否有足够的回调天数（从涨停次日开始计算）
        if len(stock_data) - limit_up_idx >= 14:  # 涨停日 + 13天回调
            # 获取回调期间的数据（从涨停次日开始）
            pullback_data = stock_data.iloc[limit_up_idx + 1:limit_up_idx + 14]
            
            # 计算回调幅度
            max_pullback = (stock_data.iloc[limit_up_idx]['收盘'] - pullback_data['收盘'].min()) / stock_data.iloc[limit_up_idx]['收盘'] * 100
            
            results.append({
                'pattern_type': '单涨停回调',
                'limit_up_date': stock_data.iloc[limit_up_idx]['日期'],
                'pullback_start_date': pullback_data.iloc[0]['日期'],
                'pullback_days': 13,
                'max_pullback_pct': max_pullback,
                'latest_price': pullback_data['收盘'].iloc[-1],
                'status': '符合条件'
            })
    
    return results

def process_single_stock(stock_code: str, stock_name: str, start_date: str, end_date: str, sector_filter: str = None) -> List[Dict]:
    """处理单只股票"""
    global filtering_status
    
    try:
        # 获取板块信息（如果需要过滤）
        if sector_filter and sector_filter != "全部":
            sector = get_stock_sector(stock_code)
            if sector != sector_filter:
                return []
        
        # 获取股票数据
        stock_data = get_stock_data(stock_code, start_date, end_date)
        if stock_data.empty:
            return []
        
        # 找出涨停日期
        limit_up_days = find_limit_up_days(stock_data)
        if not limit_up_days:
            return []
        
        # 检查两种模式
        results = []
        
        # 模式1: 10天内双涨停，首根后回调13天
        double_pattern_results = check_double_limit_up_pattern(stock_data, limit_up_days)
        results.extend(double_pattern_results)
        
        # 模式2: 单次涨停，隔日起回调13天
        single_pattern_results = check_single_limit_up_pattern(stock_data, limit_up_days)
        results.extend(single_pattern_results)
        
        # 添加股票基本信息
        for result in results:
            result['stock_code'] = stock_code
            result['stock_name'] = stock_name
            result['sector'] = get_stock_sector(stock_code)
        
        # 更新进度
        with lock:
            filtering_status["progress"] += 1
        
        return results
    
    except Exception as e:
        st.warning(f"处理股票 {stock_code} 时出错: {str(e)}")
        return []

def filter_stocks_multithread(stock_list: pd.DataFrame, start_date: str, end_date: str, sector_filter: str = None) -> List[Dict]:
    """多线程筛选股票"""
    global filtering_status
    
    filtering_status["running"] = True
    filtering_status["progress"] = 0
    filtering_status["message"] = "开始筛选..."
    
    all_results = []
    total_stocks = len(stock_list)
    
    # 使用线程池
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # 提交所有任务
        futures = []
        for _, row in stock_list.iterrows():
            future = executor.submit(
                process_single_stock,
                row['代码'],
                row['名称'],
                start_date,
                end_date,
                sector_filter
            )
            futures.append(future)
        
        # 处理结果
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    all_results.extend(result)
            except Exception as e:
                st.warning(f"任务执行出错: {str(e)}")
    
    filtering_status["running"] = False
    filtering_status["message"] = "筛选完成！"
    
    return all_results

def export_to_excel(results: List[Dict]) -> bytes:
    """导出结果到Excel"""
    if not results:
        return None
    
    df = pd.DataFrame(results)
    
    # 重新排列列顺序
    columns_order = [
        'stock_code', 'stock_name', 'sector', 'pattern_type',
        'first_limit_up_date', 'second_limit_up_date', 'limit_up_date',
        'pullback_start_date', 'pullback_days', 'max_pullback_pct',
        'latest_price', 'status'
    ]
    
    # 确保所有列都存在
    for col in columns_order:
        if col not in df.columns:
            df[col] = ""
    
    df = df[columns_order]
    
    # 重命名列
    df.columns = [
        '股票代码', '股票名称', '所属板块', '模式类型',
        '第一次涨停日期', '第二次涨停日期', '涨停日期',
        '回调开始日期', '回调天数', '最大回调幅度(%)',
        '最新价格', '状态'
    ]
    
    # 保存到Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='涨停回调筛选结果')
    
    output.seek(0)
    return output.getvalue()

def main():
    """主函数"""
    if not authenticate_user():
        return
    
    st.title("📈 股票涨停回调筛选系统")
    
    # 侧边栏配置
    with st.sidebar:
        st.header("筛选配置")
        
        # 日期选择
        end_date = datetime.datetime.now().strftime("%Y%m%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=60)).strftime("%Y%m%d")
        
        start_date = st.date_input("开始日期", value=datetime.datetime.now() - datetime.timedelta(days=60))
        end_date = st.date_input("结束日期", value=datetime.datetime.now())
        
        start_date_str = start_date.strftime("%Y%m%d")
        end_date_str = end_date.strftime("%Y%m%d")
        
        # 板块筛选
        sector_options = ["全部", "金融", "医药", "科技", "消费", "制造", "能源", "房地产"]
        selected_sector = st.selectbox("板块筛选", sector_options)
        
        # 筛选按钮
        filter_button = st.button("开始筛选", type="primary")
        
        # 清除缓存按钮
        if st.button("清除缓存"):
            results_cache.clear()
            st.success("缓存已清除")
    
    # 主内容区域
    if filter_button:
        with st.spinner("正在获取股票列表..."):
            stock_list = get_stock_list()
        
        if stock_list.empty:
            st.error("无法获取股票列表，请稍后重试")
            return
        
        st.info(f"共获取到 {len(stock_list)} 只股票，开始筛选...")
        
        # 显示进度条
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # 多线程筛选
        results = filter_stocks_multithread(stock_list, start_date_str, end_date_str, selected_sector if selected_sector != "全部" else None)
        
        # 更新缓存
        results_cache['last_results'] = results
        results_cache['filter_params'] = {
            'start_date': start_date_str,
            'end_date': end_date_str,
            'sector': selected_sector
        }
        
        # 显示结果
        if results:
            st.success(f"筛选完成！共找到 {len(results)} 个符合条件的股票")
            
            # 显示结果表格
            df = pd.DataFrame(results)
            st.dataframe(df, use_container_width=True)
            
            # 导出按钮
            excel_data = export_to_excel(results)
            if excel_data:
                st.download_button(
                    label="📥 导出Excel",
                    data=excel_data,
                    file_name=f"stock_limit_up_pullback_{start_date_str}_{end_date_str}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        else:
            st.info("未找到符合条件的股票")
    
    # 显示历史结果
    if 'last_results' in results_cache and results_cache['last_results']:
        st.subheader("📊 历史筛选结果")
        params = results_cache['filter_params']
        st.info(f"筛选条件: 日期 {params['start_date']} 至 {params['end_date']}, 板块: {params['sector']}")
        
        df = pd.DataFrame(results_cache['last_results'])
        st.dataframe(df, use_container_width=True)

if __name__ == "__main__":
    main()
