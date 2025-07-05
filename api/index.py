# api/index.py
# 修正了 10-days watchlist 的处理逻辑：不排序，直接返回扁平列表。

import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta

# --- 辅助函数：read_watchlist_from_json (保持不变) ---
def read_watchlist_from_json(file_path):
    # ... (此函数代码不变) ...
    print(f"Reading watchlist from: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f: data = json.load(f)
        watchlist = [str(item['代码']) for item in data]; print(f"Successfully read {len(watchlist)} codes.")
        return watchlist
    except FileNotFoundError:
        print(f"Warning: Watchlist file not found: {file_path}. Skipping."); return []
    except Exception as e:
        print(f"Error reading watchlist file {file_path}: {e}"); return []

# --- 核心处理函数 (get_etf_report, get_stock_report, get_hk_stock_report) ---
# ... (这三个函数的代码完全不变，为简洁省略) ...
def get_etf_report_from_akshare():
    print("--- (1/5) Processing ETF Data ---")
    df_raw = ak.fund_etf_spot_em()
    if df_raw.empty: raise RuntimeError("Empty ETF DataFrame.")
    try:
        ts = pd.to_datetime(df_raw['数据日期'].iloc[0]); trade_date = ts.strftime('%Y-%m-%d')
    except:
        trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']
    df = df_raw[required_columns].copy(); df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['Price'] = df['Price'].round(3); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data."}, trade_date
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report, trade_date

def get_stock_report_from_akshare(trade_date):
    print("\n--- (2/5) Processing All A-Share Stock Data ---")
    df_raw = ak.stock_zh_a_spot_em()
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
    print("\n--- (3/5) Processing All Hong Kong Stock Data ---")
    df_raw = ak.stock_hk_main_board_spot_em()
    if df_raw.empty: raise RuntimeError("Empty HK Stock DataFrame.")
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']; df = df_raw[required_columns].copy()
    df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['Price'] = df['Price'].round(3); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report

# --- 修改后的函数：处理 A 股“关注列表” ---
def get_stock_watchlist_report(trade_date, watchlist_codes):
    """
    基于给定的股票代码名单，筛选A股实时行情并按原始顺序返回扁平列表。
    """
    print("\n--- (4/5) Processing A-Share Watchlist Data ---")
    if not watchlist_codes:
        return {"error": "Watchlist is empty, skipping."}
        
    df_raw = ak.stock_zh_a_spot_em()
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for stocks.")
    
    # 将实时行情数据设置为以'代码'为索引的字典，方便快速查找
    df_raw['代码'] = df_raw['代码'].astype(str)
    live_data_map = df_raw.set_index('代码').to_dict('index')

    # 按照 watchlist_codes 的顺序来构建结果
    result_list = []
    for code in watchlist_codes:
        if code in live_data_map:
            item = live_data_map[code]
            stock_info = {
                '代码': code,
                '名称': item.get('名称'),
                'Price': round(pd.to_numeric(item.get('最新价'), errors='coerce'), 2),
                'Percent': round(pd.to_numeric(item.get('涨跌幅'), errors='coerce'), 2),
                'Amount': round(pd.to_numeric(item.get('成交额'), errors='coerce') / 100_000_000, 2),
                'PE_TTM': round(pd.to_numeric(item.get('市盈率-动态'), errors='coerce'), 2),
                'PB': round(pd.to_numeric(item.get('市净率'), errors='coerce'), 2),
                'TotalMarketCap': round(pd.to_numeric(item.get('总市值'), errors='coerce') / 100_000_000, 2),
            }
            # 增加一个更新时间和交易日字段
            stock_info["update_time_bjt"] = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
            stock_info["trade_date"] = trade_date
            result_list.append(stock_info)
        else:
            print(f"  Warning: Code {code} from watchlist not found in live market data.")

    print(f"Processed {len(result_list)} stocks from the A-Share watchlist.")
    # 直接返回处理后的列表
    return result_list

# --- 修改后的函数：处理港股“关注列表” ---
def get_hk_stock_watchlist_report(trade_date, watchlist_codes):
    """
    基于给定的股票代码名单，筛选港股实时行情并按原始顺序返回扁平列表。
    """
    print("\n--- (5/5) Processing Hong Kong Stock Watchlist Data ---")
    if not watchlist_codes:
        return {"error": "Watchlist is empty, skipping."}
        
    df_raw = ak.stock_hk_main_board_spot_em()
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame for HK stocks.")
        
    # 将实时行情数据设置为以'代码'为索引的字典
    df_raw['代码'] = df_raw['代码'].astype(str)
    live_data_map = df_raw.set_index('代码').to_dict('index')
    
    # 按照 watchlist_codes 的顺序来构建结果
    result_list = []
    for code in watchlist_codes:
        if code in live_data_map:
            item = live_data_map[code]
            stock_info = {
                '代码': code,
                '名称': item.get('名称'),
                'Price': round(pd.to_numeric(item.get('最新价'), errors='coerce'), 3),
                'Percent': round(pd.to_numeric(item.get('涨跌幅'), errors='coerce'), 2),
                'Amount': round(pd.to_numeric(item.get('成交额'), errors='coerce') / 100_000_000, 2),
            }
            stock_info["update_time_bjt"] = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
            stock_info["trade_date"] = trade_date
            result_list.append(stock_info)
        else:
            print(f"  Warning: Code {code} from watchlist not found in live market data.")
            
    print(f"Processed {len(result_list)} stocks from the HK Stock watchlist.")
    # 直接返回处理后的列表
    return result_list

# --- 脚本执行入口 (保持不变) ---
if __name__ == "__main__":
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    
    base_trade_date = None
    
    def run_and_save_task(name, func, file, *args):
        # ... (此辅助函数代码不变) ...
        output_filepath = os.path.join(output_dir, file)
        final_data = {}; print(f"\nWriting {name} data to {output_filepath}...")
        try: final_data = func(*args)
        except Exception as e: print(f"Error during {name}: {e}"); final_data = {"error": str(e)}
        with open(output_filepath, 'w', encoding='utf-8') as f: json.dump(final_data, f, ensure_ascii=False, indent=4)
        print(f"{name} data file generated successfully.")

    etf_data, base_trade_date = {}, datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    try:
        etf_data, base_trade_date = get_etf_report_from_akshare()
    except Exception as e:
        print(f"Error during ETF processing: {e}"); etf_data = {"error": str(e)}
    run_and_save_task("ETF", lambda: etf_data, "etf_data.json")
    
    run_and_save_task("A-Share Stock", get_stock_report_from_akshare, "stock_data.json", base_trade_date)
    run_and_save_task("Hong Kong Stock", get_hk_stock_report_from_akshare, "hk_stock_data.json", base_trade_date)
    
    a_share_watchlist = read_watchlist_from_json(os.path.join(output_dir, "ARHot10days_top20.json"))
    hk_share_watchlist = read_watchlist_from_json(os.path.join(output_dir, "HKHot10days_top20.json"))
    
    run_and_save_task("A-Share Watchlist", get_stock_watchlist_report, "stock_10days_data.json", base_trade_date, a_share_watchlist)
    run_and_save_task("HK Stock Watchlist", get_hk_stock_watchlist_report, "hk_stock_10days_data.json", base_trade_date, hk_share_watchlist)

    print("\nAll tasks finished.")
