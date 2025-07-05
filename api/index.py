# api/index.py
# 统一日期处理逻辑：以ETF数据日期为基准，传递给其他函数。

import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta

# --- 函数 1: 获取 ETF 数据报告 (现在会返回日期) ---
def get_etf_report_from_akshare():
    """
    获取ETF数据，并返回报告字典和提取出的交易日期。
    """
    print("--- (1/3) Processing ETF Data ---")
    df_raw = ak.fund_etf_spot_em()
    print(f"Successfully fetched {len(df_raw)} ETFs from akshare.")
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for ETFs.")
    
    # 提取交易日期
    try:
        timestamp_obj = pd.to_datetime(df_raw['数据日期'].iloc[0])
        trade_date = timestamp_obj.strftime('%Y-%m-%d')
        print(f"Extracted base trade date from ETF data source: {trade_date}")
    except (KeyError, IndexError):
        trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
        print(f"Warning: ETF '数据日期' not found. Using current date as base: {trade_date}")

    # ... (数据处理部分保持不变) ...
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
        return {"error": "No valid ETF data after cleaning."}, trade_date

    df_top_up = df.sort_values(by='Percent', ascending=False).head(20)
    df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    
    report = {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        "trade_date": trade_date, # 使用提取出的日期
        "top_up_20": df_top_up.to_dict('records'),
        "top_down_20": df_top_down.to_dict('records')
    }
    # 返回报告和日期
    return report, trade_date

# --- 函数 2: 获取 A 股数据报告 (现在接收 trade_date 参数) ---
def get_stock_report_from_akshare(trade_date):
    """
    获取A股实时行情，并使用传入的交易日期生成报告。
    """
    print("\n--- (2/3) Processing A-Share Stock Data ---")
    df_raw = ak.stock_zh_a_spot_em()
    print(f"Successfully fetched {len(df_raw)} stocks from akshare.")
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for stocks.")
    
    # ... (数据处理部分保持不变) ...
    df_filtered = df_raw[
        ~df_raw['代码'].str.startswith(('4', '8')) &
        ~df_raw['名称'].str.contains('ST|退')
    ].copy()
    print(f"Filtered down to {len(df_filtered)} stocks.")
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
        "trade_date": trade_date, # 使用传入的日期
        "top_up_20": df_top_up.to_dict('records'),
        "top_down_20": df_top_down.to_dict('records')
    }
    return report

# --- 函数 3: 获取港股数据报告 (现在接收 trade_date 参数) ---
def get_hk_stock_report_from_akshare(trade_date):
    """
    获取港股主板实时行情，并使用传入的交易日期生成报告。
    """
    print("\n--- (3/3) Processing Hong Kong Stock Data ---")
    df_raw = ak.stock_hk_main_board_spot_em()
    print(f"Successfully fetched {len(df_raw)} HK stocks from akshare.")
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for HK stocks.")
        
    # ... (数据处理部分保持不变) ...
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']
    df = df_raw[required_columns].copy()
    df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True)
    df['Amount'] = (df['Amount'] / 100_000_000).round(2)
    df['Price'] = df['Price'].round(3)
    df['Percent'] = df['Percent'].round(2)
    
    if df.empty:
        return {"error": "No valid HK stock data after cleaning."}
        
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20)
    df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    
    report = {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        "trade_date": trade_date, # 使用传入的日期
        "top_up_20": df_top_up.to_dict('records'),
        "top_down_20": df_top_down.to_dict('records')
    }
    return report

# --- 脚本执行入口 (已修改，实现日期传递) ---
if __name__ == "__main__":
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    
    base_trade_date = None

    # --- 1. 处理 ETF 数据并获取基准日期 ---
    etf_output_filepath = os.path.join(output_dir, "etf_data.json")
    etf_final_data = {}
    try:
        etf_final_data, base_trade_date = get_etf_report_from_akshare()
    except Exception as e:
        print(f"\nAn critical error occurred during ETF data processing: {e}")
        etf_final_data = {"error": str(e)}
        # 即使获取ETF失败，也要设置一个备用日期
        base_trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
        print(f"Using fallback trade date for subsequent tasks: {base_trade_date}")

    print(f"\nWriting ETF data to {etf_output_filepath}...")
    with open(etf_output_filepath, 'w', encoding='utf-8') as f:
        json.dump(etf_final_data, f, ensure_ascii=False, indent=4)
    print("ETF data file generated successfully.")

    # --- 2. 处理 A 股数据，传入基准日期 ---
    stock_output_filepath = os.path.join(output_dir, "stock_data.json")
    stock_final_data = {}
    try:
        stock_final_data = get_stock_report_from_akshare(base_trade_date)
    except Exception as e:
        print(f"\nAn critical error occurred during A-Share Stock data processing: {e}")
        stock_final_data = {"error": str(e)}

    print(f"\nWriting A-Share Stock data to {stock_output_filepath}...")
    with open(stock_output_filepath, 'w', encoding='utf-8') as f:
        json.dump(stock_final_data, f, ensure_ascii=False, indent=4)
    print("A-Share Stock data file generated successfully.")

    # --- 3. 处理港股数据，传入基准日期 ---
    hk_stock_output_filepath = os.path.join(output_dir, "hk_stock_data.json")
    hk_final_data = {}
    try:
        hk_final_data = get_hk_stock_report_from_akshare(base_trade_date)
    except Exception as e:
        print(f"\nAn critical error occurred during Hong Kong Stock data processing: {e}")
        hk_final_data = {"error": str(e)}

    print(f"\nWriting Hong Kong Stock data to {hk_stock_output_filepath}...")
    with open(hk_stock_output_filepath, 'w', encoding='utf-8') as f:
        json.dump(hk_final_data, f, ensure_ascii=False, indent=4)
    print("Hong Kong Stock data file generated successfully.")

    print("\nAll tasks finished.")
