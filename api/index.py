# api/index.py
# 这个脚本现在可以同时处理 ETF、A股和港股主板数据，
# 并分别生成 etf_data.json, stock_data.json, 和 hk_stock_data.json 三个文件。

import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta

# --- 函数 1: 获取 ETF 数据报告 (保持不变) ---
def get_etf_report_from_akshare():
    """
    使用 akshare.fund_etf_spot_em() 获取实时ETF数据，
    并整理成包含涨跌幅 Top 20 的报告字典。
    """
    print("--- (1/3) Processing ETF Data ---")
    print("Attempting to fetch ETF data using akshare.fund_etf_spot_em...")
    df_raw = ak.fund_etf_spot_em()
    print(f"Successfully fetched {len(df_raw)} ETFs from akshare.")
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for ETFs.")
    try:
        timestamp_obj = pd.to_datetime(df_raw['数据日期'].iloc[0])
        trade_date = timestamp_obj.strftime('%Y-%m-%d')
        print(f"Extracted ETF trade date from data source: {trade_date}")
    except (KeyError, IndexError):
        trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
        print(f"Warning: ETF '数据日期' not found. Falling back to current date: {trade_date}")
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']
    df = df_raw[required_columns].copy()
    df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Percent'] = pd.to_numeric(df['Percent'], errors='coerce')
    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
    df.dropna(inplace=True)
    df['Amount'] = (df['Amount'] / 100_000_000).round(2)
    df['Price'] = df['Price'].round(3)
    df['Percent'] = df['Percent'].round(2)
    if df.empty:
        return {"error": "No valid ETF data after cleaning."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20)
    df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        "trade_date": trade_date,
        "top_up_20": df_top_up.to_dict('records'),
        "top_down_20": df_top_down.to_dict('records')
    }
    return report

# --- 函数 2: 获取 A 股数据报告 (保持不变) ---
def get_stock_report_from_akshare():
    """
    使用 akshare.stock_zh_a_spot_em() 获取A股实时行情数据，
    并整理成包含涨跌幅 Top 20 的报告字典。
    """
    print("\n--- (2/3) Processing A-Share Stock Data ---")
    print("Attempting to fetch A-share stock data using akshare.stock_zh_a_spot_em...")
    df_raw = ak.stock_zh_a_spot_em()
    print(f"Successfully fetched {len(df_raw)} stocks from akshare.")
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for stocks.")
    df_filtered = df_raw[
        ~df_raw['代码'].str.startswith(('4', '8')) &
        ~df_raw['名称'].str.contains('ST|退')
    ].copy()
    print(f"Filtered down to {len(df_filtered)} stocks (removed BJSE, ST, and delisting).")
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额', '市盈率-动态', '市净率', '总市值']
    df = df_filtered[required_columns].copy()
    df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount', '市盈率-动态': 'PE_TTM', '市净率': 'PB', '总市值': 'TotalMarketCap'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount', 'PE_TTM', 'PB', 'TotalMarketCap']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True)
    df['Amount'] = (df['Amount'] / 100_000_000).round(2)
    df['TotalMarketCap'] = (df['TotalMarketCap'] / 100_000_000).round(2)
    df['Price'] = df['Price'].round(2)
    df['Percent'] = df['Percent'].round(2)
    if df.empty:
        return {"error": "No valid stock data after cleaning."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20)
    df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        "trade_date": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d'),
        "top_up_20": df_top_up.to_dict('records'),
        "top_down_20": df_top_down.to_dict('records')
    }
    return report

# --- 函数 3: 新增 - 获取港股数据报告 ---
def get_hk_stock_report_from_akshare():
    """
    使用 akshare.stock_hk_main_board_spot_em() 获取港股主板实时行情，
    并整理成包含涨跌幅 Top 20 的报告字典。
    """
    print("\n--- (3/3) Processing Hong Kong Stock Data ---")
    print("Attempting to fetch HK stock data using akshare.stock_hk_main_board_spot_em...")

    # 1. 获取数据
    df_raw = ak.stock_hk_main_board_spot_em()
    print(f"Successfully fetched {len(df_raw)} HK stocks from akshare.")

    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for HK stocks.")

    # 2. 数据清洗和重命名
    # 港股接口列名: '代码', '名称', '最新价', '涨跌额', '涨跌幅', '昨收', '今开', '最高', '最低', '成交量', '成交额'
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']
    df = df_raw[required_columns].copy()
    df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    
    # 3. 数据类型转换和格式化
    for col in ['Price', 'Percent', 'Amount']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True)

    # 港股的成交额单位通常是“港元”，我们转换为“亿港元”
    df['Amount'] = (df['Amount'] / 100_000_000).round(2)
    df['Price'] = df['Price'].round(3) # 港股股价可以有3位小数
    df['Percent'] = df['Percent'].round(2)
    
    if df.empty:
        return {"error": "No valid HK stock data after cleaning."}
        
    # 4. 生成 Top 20 榜单
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20)
    df_top_down = df.sort_values(by='Percent', ascending=True).head(20)

    # 5. 构建报告字典
    report = {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        "trade_date": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d'),
        "top_up_20": df_top_up.to_dict('records'),
        "top_down_20": df_top_down.to_dict('records')
    }
    return report

# --- 脚本执行入口 (已修改，增加港股处理) ---
if __name__ == "__main__":
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    # 定义任务列表，每个任务包含处理函数和输出文件名
    tasks = [
        {"name": "ETF", "func": get_etf_report_from_akshare, "file": "etf_data.json"},
        {"name": "A-Share Stock", "func": get_stock_report_from_akshare, "file": "stock_data.json"},
        {"name": "Hong Kong Stock", "func": get_hk_stock_report_from_akshare, "file": "hk_stock_data.json"},
    ]

    # 循环执行所有任务
    for task in tasks:
        output_filepath = os.path.join(output_dir, task["file"])
        final_data = {}
        try:
            final_data = task["func"]()
        except Exception as e:
            print(f"\nAn critical error occurred during {task['name']} data processing: {e}")
            final_data = {
                "error": str(e),
                "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
            }
        
        print(f"\nWriting {task['name']} data to {output_filepath}...")
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        print(f"{task['name']} data file generated successfully.")

    print("\nAll tasks finished.")
