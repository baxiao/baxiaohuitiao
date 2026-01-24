import baostock as bs
import pandas as pd
from datetime import datetime, timedelta

def stock_screening_callback_logic():
    # 1. 登录系统
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        return

    # 设置回溯时间：取30个交易日以确保计算滑窗充足
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")

    print(f"正在筛选：13个交易日内仅有一次涨停的个股 (当前日期: {end_date})...")

    # 2. 获取所有股票列表
    rs = bs.query_all_stock(day=end_date)
    all_stocks = []
    while (rs.error_code == '0') & rs.next():
        all_stocks.append(rs.get_row_data())
    
    result_df = pd.DataFrame(all_stocks, columns=rs.fields)
    final_list = []

    # 3. 遍历股票进行逻辑筛选
    for index, row in result_df.iterrows():
        code = row['code']
        code_name = row['code_name']

        # --- 规则过滤：剔除 ST、创业板、科创板 ---
        if "ST" in code_name or "st" in code_name:
            continue
        raw_code = code.split('.')[-1]
        if raw_code.startswith('300') or raw_code.startswith('688'):
            continue
        # ---------------------------------------

        # 获取历史K线数据
        k_rs = bs.query_history_k_data_plus(
            code, "date,code,close,preclose,pctChg",
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag="3"
        )
        
        k_data = []
        while (k_rs.error_code == '0') & k_rs.next():
            k_data.append(k_rs.get_row_data())
        
        # 观察窗：至少需要14天（1天涨停 + 13天回调）
        if len(k_data) < 14:
            continue

        df_stock = pd.DataFrame(k_data, columns=k_rs.fields)
        df_stock['pctChg'] = pd.to_numeric(df_stock['pctChg'])
        
        # --- 核心修改：仅保留单次涨停逻辑，剔除连阳限制 ---
        # 截取最近14个交易日
        recent_window = df_stock.tail(14).copy()
        limit_up_mask = recent_window['pctChg'] >= 9.9
        limit_up_count = limit_up_mask.sum()

        # 条件：这14天内有且仅有一次涨停
        if limit_up_count == 1:
            # 找到那次涨停的索引位置
            limit_up_idx = recent_window[limit_up_mask].index[0]
            # 计算涨停至今经过了多少个交易日
            days_passed = (len(df_stock) - 1) - limit_up_idx
            
            final_list.append({
                "代码": code,
                "名称": code_name,
                "现价": recent_window.iloc[-1]['close'],
                "今日涨幅": f"{recent_window.iloc[-1]['pctChg']}%",
                "距涨停已过天数": days_passed
            })

    # 4. 输出结果
    print("\n" + "="*65)
    print(f"{'序号':^6} | {'代码':^10} | {'名称':^10} | {'距涨停天数':^10} | {'当前状态':^10}")
    print("-" * 65)
    
    if final_list:
        for i, item in enumerate(final_list, 1):
            # 统一展示格式
            print(f"{i:^8} | {item['代码']:^10} | {item['名称']:^10} | {item['距涨停已过天数']:^12} | {item['今日涨幅']:^12}")
    else:
        print("未发现符合“单次涨停+13日内回调”条件的股票。")
    print("="*65)

    bs.logout()

if __name__ == "__main__":
    stock_screening_callback_logic()
