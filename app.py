import baostock as bs
import pandas as pd
from datetime import datetime, timedelta

def stock_screening_single_limit_up():
    # 1. 登录系统
    lg = bs.login()
    if lg.error_code != '0':
        print(f"登录失败: {lg.error_msg}")
        return

    # 设置回溯时间：为了计算13天回调+单次涨停，取最近25个交易日
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d")

    print(f"筛选中：单次涨停后第13天进入调整/连阳 (日期: {end_date})...")

    # 2. 获取所有股票列表
    rs = bs.query_all_stock(day=end_date)
    all_stocks = []
    while (rs.error_code == '0') & rs.next():
        all_stocks.append(rs.get_row_data())
    
    result_df = pd.DataFrame(all_stocks, columns=rs.fields)
    final_list = []

    # 3. 遍历股票
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

        # 获取历史K线
        k_rs = bs.query_history_k_data_plus(
            code, "date,code,close,preclose,pctChg",
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag="3"
        )
        
        k_data = []
        while (k_rs.error_code == '0') & k_rs.next():
            k_data.append(k_rs.get_row_data())
        
        if len(k_data) < 15:
            continue

        df_stock = pd.DataFrame(k_data, columns=k_rs.fields)
        df_stock['pctChg'] = pd.to_numeric(df_stock['pctChg'])
        
        # --- 核心逻辑修改：单次涨停且后续回调 ---
        # 我们观察最近14个交易日的数据（1个涨停位 + 13天回调位）
        obs_window = df_stock.tail(14).copy()
        limit_up_count = (obs_window['pctChg'] >= 9.9).sum()

        # 条件1：有且仅有一次涨停
        if limit_up_count == 1:
            # 找到涨停在那一天
            limit_up_index = obs_window[obs_window['pctChg'] >= 9.9].index[0]
            # 计算从涨停那天到现在的天数（如果正好是13天左右回调）
            days_since_limit = (len(df_stock) - 1) - limit_up_index
            
            # 条件2：涨停后经历了调整，且目前处于连阳或企稳状态 (最近3天收盘价不低于前一天)
            last_3_days = df_stock.tail(3)
            is_up_trend = (last_3_days['pctChg'] >= 0).all() 

            if is_up_trend:
                final_list.append({
                    "代码": code,
                    "名称": code_name,
                    "现价": last_3_days.iloc[-1]['close'],
                    "涨停距今天数": days_since_limit,
                    "今日涨幅": f"{last_3_days.iloc[-1]['pctChg']}%"
                })

    # 4. 输出结果
    print("\n" + "="*65)
    print(f"{'序号':^6} | {'代码':^10} | {'名称':^10} | {'距涨停天数':^10} | {'今日涨幅':^8}")
    print("-" * 65)
    
    if final_list:
        for i, item in enumerate(final_list, 1):
            print(f"{i:^8} | {item['代码']:^10} | {item['名称']:^10} | {item['涨停距今天数']:^12} | {item['今日涨幅']:^10}")
    else:
        print("未发现符合“单次涨停+13日回调连阳”条件的股票。")
    print("="*65)

    bs.logout()

if __name__ == "__main__":
    stock_screening_single_limit_up()
