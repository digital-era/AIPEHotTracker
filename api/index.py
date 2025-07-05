# api/index.py
# 增加了基于特定股票名单进行二次筛选和处理的功能。

import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta

# --- 辅助函数：从 JSON 文件读取股票代码名单 ---
def read_watchlist_from_json(file_path):
    """
    读取指定的 JSON 文件，并提取所有 '代码' 字段的值。
    """
    print(f"Reading watchlist from: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # 提取'代码'列，并转换为字符串列表，以防万一是数字
        watchlist = [str(item['代码']) for item in data]
        print(f"Successfully read {len(watchlist)} codes from watchlist.")
        return watchlist
    except FileNotFoundError:
        print(f"Warning: Watchlist file not found: {file_path}. Skipping this task.")
        return []
    except Exception as e:
        print(f"Error reading watchlist file {file_path}: {e}")
        return []

# --- 核心处理函数 (基本保持不变) ---
# ... (get_etf_report_from_akshare, get_stock_report_from_akshare, get_hk_stock_report_from_akshare 函数代码不变) ...
# 为了简洁，这里省略了这三个函数的完整代码，请使用你提供的最新版本。
def get_etf_report_from_akshare():
    """获取ETF数据，并返回报告字典和提取出的交易日期。"""
    print("--- (1/5) Processing ETF Data ---")
    df_raw = ak.fund_etf_spot_em()
    print(f"Successfully fetched {len(df_raw)} ETFs.")
    # ... (其余代码不变)
    if df_raw.empty: raise RuntimeError("Empty ETF DataFrame.")
    try:
        ts = pd.to_datetime(df_raw['数据日期'].iloc[0])
        trade_date = ts.strftime('%Y-%m-%d')
    except:
        trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    # ... (数据处理代码不变)
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']
    df = df_raw[required_columns].copy(); df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce'); df['Percent'] = pd.to_numeric(df['Percent'], errors='coerce'); df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['Price'] = df['Price'].round(3); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data."}, trade_date
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report, trade_date

def get_stock_report_from_akshare(trade_date):
    """获取A股实时行情，并使用传入的交易日期生成报告。"""
    print("\n--- (2/5) Processing All A-Share Stock Data ---")
    df_raw = ak.stock_zh_a_spot_em()
    print(f"Successfully fetched {len(df_raw)} stocks.")
    # ... (其余代码不变)
    if df_raw.empty: raise RuntimeError("Empty Stock DataFrame.")
    df_filtered = df_raw[~df_raw['代码'].str.startswith(('4', '8')) & ~df_raw['名称'].str.contains('ST|退')].copy()
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额', '市盈率-动态', '市净率', '总市值']
    df = df_filtered[required_columns].copy(); df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount', '市盈率-动态': 'PE_TTM', '市净率': 'PB', '总市值': 'TotalMarketCap'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount', 'PE_TTM', 'PB', 'TotalMarketCap']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['TotalMarketCap'] = (df['TotalMarketCap'] / 100_000_000).round(2); df['Price'] = df['Price'].round(2); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report

def get_hk_stock_report_from_akshare(trade_date):
    """获取港股主板实时行情，并使用传入的交易日期生成报告。"""
    print("\n--- (3/5) Processing All Hong Kong Stock Data ---")
    df_raw = ak.stock_hk_main_board_spot_em()
    print(f"Successfully fetched {len(df_raw)} HK stocks.")
    # ... (其余代码不变)
    if df_raw.empty: raise RuntimeError("Empty HK Stock DataFrame.")
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']; df = df_raw[required_columns].copy()
    df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['Price'] = df['Price'].round(3); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report

# --- 新增函数：处理 A 股“关注列表” ---
def get_stock_watchlist_report(trade_date, watchlist_codes):
    """
    基于给定的股票代码名单，筛选A股实时行情并生成Top 20报告。
    """
    print("\n--- (4/5) Processing A-Share Watchlist Data ---")
    if not watchlist_codes:
        return {"error": "Watchlist is empty, skipping."}
        
    df_raw = ak.stock_zh_a_spot_em()
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for stocks.")
    
    # 核心逻辑：筛选出代码在 watchlist 中的股票
    # 注意：确保两边代码都是字符串类型以正确匹配
    df_watchlist = df_raw[df_raw['代码'].astype(str).isin(watchlist_codes)].copy()
    print(f"Found {len(df_watchlist)} stocks from the watchlist in the live market data.")

    if df_watchlist.empty:
        return {"error": "None of the stocks in the watchlist were found in the live market data."}
    
    # 后续处理逻辑与 get_stock_report_from_akshare 完全相同
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额', '市盈率-动态', '市净率', '总市值']
    df = df_watchlist[required_columns].copy()
    df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount', '市盈率-动态': 'PE_TTM', '市净率': 'PB', '总市值': 'TotalMarketCap'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount', 'PE_TTM', 'PB', 'TotalMarketCap']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['TotalMarketCap'] = (df['TotalMarketCap'] / 100_000_000).round(2); df['Price'] = df['Price'].round(2); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data after cleaning from watchlist."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report

# --- 新增函数：处理港股“关注列表” ---
def get_hk_stock_watchlist_report(trade_date, watchlist_codes):
    """
    基于给定的股票代码名单，筛选港股实时行情并生成Top 20报告。
    """
    print("\n--- (5/5) Processing Hong Kong Stock Watchlist Data ---")
    if not watchlist_codes:
        return {"error": "Watchlist is empty, skipping."}
        
    df_raw = ak.stock_hk_main_board_spot_em()
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for HK stocks.")
        
    # 核心逻辑：筛选出代码在 watchlist 中的股票
    df_watchlist = df_raw[df_raw['代码'].astype(str).isin(watchlist_codes)].copy()
    print(f"Found {len(df_watchlist)} HK stocks from the watchlist in the live market data.")

    if df_watchlist.empty:
        return {"error": "None of the stocks in the watchlist were found in the live market data."}
    
    # 后续处理逻辑与 get_hk_stock_report_from_akshare 完全相同
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']; df = df_watchlist[required_columns].copy()
    df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['Price'] = df['Price'].round(3); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data after cleaning from watchlist."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report

# --- 脚本执行入口 (已重构，支持所有5个任务) ---
if __name__ == "__main__":
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    
    base_trade_date = None
    
    # 定义一个辅助函数来执行和保存任务
    def run_and_save_task(name, func, file, *args):
        output_filepath = os.path.join(output_dir, file)
        final_data = {}
        try:
            final_data = func(*args)
        except Exception as e:
            print(f"\nAn critical error occurred during {name} data processing: {e}")
            final_data = {"error": str(e)}
        
        print(f"\nWriting {name} data to {output_filepath}...")
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        print(f"{name} data file generated successfully.")
        return final_data

    # --- 任务执行阶段 ---
    
    # 1. 基础任务：获取 ETF 数据并确定基准日期
    etf_data, base_trade_date = {}, datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    try:
        etf_data, base_trade_date = get_etf_report_from_akshare()
    except Exception as e:
        print(f"\nAn critical error occurred during ETF data processing: {e}")
        etf_data = {"error": str(e)}
        print(f"Using fallback trade date for subsequent tasks: {base_trade_date}")
    run_and_save_task("ETF", lambda: etf_data, "etf_data.json")

    # 2. 基础任务：获取全市场 A 股和港股数据
    run_and_save_task("A-Share Stock", get_stock_report_from_akshare, "stock_data.json", base_trade_date)
    run_and_save_task("Hong Kong Stock", get_hk_stock_report_from_akshare, "hk_stock_data.json", base_trade_date)
    
    # 3. 读取关注列表 (假设这两个文件已经由另一个流程生成并存在于 data 目录)
    a_share_watchlist = read_watchlist_from_json(os.path.join(output_dir, "ARHot10days_top20.json"))
    hk_share_watchlist = read_watchlist_from_json(os.path.join(output_dir, "HKHot10days_top20.json"))
    
    # 4. 二次加工任务：基于关注列表生成报告
    run_and_save_task("A-Share Watchlist", get_stock_watchlist_report, "stock_10days_data.json", base_trade_date, a_share_watchlist)
    run_and_save_task("HK Stock Watchlist", get_hk_stock_watchlist_report, "hk_stock_10days_data.json", base_trade_date, hk_share_watchlist)

    print("\nAll tasks finished.")
