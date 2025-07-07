# api/index.py
# 性能优化版：一次获取，多次使用，避免重复的网络请求。

import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta

# --- 辅助函数 (保持不变) ---
def read_watchlist_from_json(file_path):
    """从指定的JSON文件读取一个包含代码的列表。"""
    print(f"Reading watchlist from: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        watchlist = [str(item['代码']) for item in data]
        print(f"Successfully read {len(watchlist)} codes.")
        return watchlist
    except FileNotFoundError:
        print(f"Warning: Watchlist file not found: {file_path}. Skipping.")
        return []
    except Exception as e:
        print(f"Error reading watchlist file {file_path}: {e}")
        return []

# --- 原有处理函数 (保持不变) ---
def process_etf_report(df_raw, trade_date):
    """处理传入的ETF DataFrame并生成报告。"""
    print("--- (1/x) Processing ETF Data ---")
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']
    df = df_raw[required_columns].copy(); df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['Price'] = df['Price'].round(3); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report

def process_stock_report(df_raw, trade_date):
    """处理传入的全市场A股DataFrame并生成报告。"""
    print("\n--- (2/x) Processing All A-Share Stock Data ---")
    df_filtered = df_raw[~df_raw['代码'].str.startswith(('4', '8')) & ~df_raw['名称'].str.contains('ST|退')].copy()
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额', '市盈率-动态', '市净率', '总市值']
    df = df_filtered[required_columns].copy(); df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount', '市盈率-动态': 'PE_TTM', '市净率': 'PB', '总市值': 'TotalMarketCap'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount', 'PE_TTM', 'PB', 'TotalMarketCap']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['TotalMarketCap'] = (df['TotalMarketCap'] / 100_000_000).round(2); df['Price'] = df['Price'].round(2); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report

def process_hk_stock_report(df_raw, trade_date):
    """处理传入的全市场港股DataFrame并生成报告。"""
    print("\n--- (3/x) Processing All Hong Kong Stock Data ---")
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']; df = df_raw[required_columns].copy()
    df.rename(columns={'最新价': 'Price', '涨跌幅': 'Percent', '成交额': 'Amount'}, inplace=True)
    for col in ['Price', 'Percent', 'Amount']: df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True); df['Amount'] = (df['Amount'] / 100_000_000).round(2); df['Price'] = df['Price'].round(3); df['Percent'] = df['Percent'].round(2)
    if df.empty: return {"error": "No valid data."}
    df_top_up = df.sort_values(by='Percent', ascending=False).head(20); df_top_down = df.sort_values(by='Percent', ascending=True).head(20)
    report = {"update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date, "top_up_20": df_top_up.to_dict('records'), "top_down_20": df_top_down.to_dict('records')}
    return report

def process_stock_watchlist_report(df_raw, trade_date, watchlist_codes):
    """在传入的A股DataFrame上，基于watchlist筛选并生成报告。"""
    print("\n--- (4/x) Processing A-Share Watchlist Data ---")
    if not watchlist_codes: return {"error": "Watchlist is empty, skipping."}
    df_raw['代码'] = df_raw['代码'].astype(str); live_data_map = df_raw.set_index('代码').to_dict('index'); result_list = []
    for code in watchlist_codes:
        if code in live_data_map:
            item = live_data_map[code]
            stock_info = {'代码': code, '名称': item.get('名称'), 'Price': round(pd.to_numeric(item.get('最新价'), errors='coerce'), 2), 'Percent': round(pd.to_numeric(item.get('涨跌幅'), errors='coerce'), 2), 'Amount': round(pd.to_numeric(item.get('成交额'), errors='coerce') / 100_000_000, 2), 'PE_TTM': round(pd.to_numeric(item.get('市盈率-动态'), errors='coerce'), 2), 'PB': round(pd.to_numeric(item.get('市净率'), errors='coerce'), 2), 'TotalMarketCap': round(pd.to_numeric(item.get('总市值'), errors='coerce') / 100_000_000, 2), "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date}
            result_list.append(stock_info)
    print(f"Processed {len(result_list)} stocks from the A-Share watchlist."); return result_list

def process_hk_stock_watchlist_report(df_raw, trade_date, watchlist_codes):
    """在传入的港股DataFrame上，基于watchlist筛选并生成报告。"""
    print("\n--- (5/x) Processing Hong Kong Stock Watchlist Data ---")
    if not watchlist_codes: return {"error": "Watchlist is empty, skipping."}
    df_raw['代码'] = df_raw['代码'].astype(str); live_data_map = df_raw.set_index('代码').to_dict('index'); result_list = []
    for code in watchlist_codes:
        if code in live_data_map:
            item = live_data_map[code]
            stock_info = {'代码': code, '名称': item.get('名称'), 'Price': round(pd.to_numeric(item.get('最新价'), errors='coerce'), 3), 'Percent': round(pd.to_numeric(item.get('涨跌幅'), errors='coerce'), 2), 'Amount': round(pd.to_numeric(item.get('成交额'), errors='coerce') / 100_000_000, 2), "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date}
            result_list.append(stock_info)
    print(f"Processed {len(result_list)} stocks from the HK Stock watchlist."); return result_list


# --- >>> 新增功能函数 (已升级，包含港股处理) <<< ---
def process_observe_list_report(df_stock_raw, df_etf_raw, df_hk_stock_raw, trade_date, observe_list_codes):
    """
    根据一个混合观察列表（含A股、港股、ETF），从全量数据中筛选并生成统一格式的报告。
    非股票标的缺失的字段（PE, PB, TotalMarketCap）将被赋值为None，以在JSON中生成null。
    """
    print("\n--- (NEW TASK) Processing Unified Observe List (A-Share, HK-Stock, ETF) ---")
    
    if not observe_list_codes:
        print("Observe list is empty, skipping.")
        return {"error": "Observe list is empty, skipping."}

    # 为所有数据源创建高效的查找字典（哈希图）
    df_stock_raw['代码'] = df_stock_raw['代码'].astype(str)
    stock_data_map = df_stock_raw.set_index('代码').to_dict('index')
    
    df_etf_raw['代码'] = df_etf_raw['代码'].astype(str)
    etf_data_map = df_etf_raw.set_index('代码').to_dict('index')

    df_hk_stock_raw['代码'] = df_hk_stock_raw['代码'].astype(str)
    hk_stock_data_map = df_hk_stock_raw.set_index('代码').to_dict('index')

    result_list = []
    
    # 辅助函数，用于安全地转换和舍入数值，处理NaN为None
    def safe_round(value, digits, divisor=1):
        numeric_val = pd.to_numeric(value, errors='coerce')
        if pd.isna(numeric_val):
            return None  # 返回None，将在JSON中变为null
        return round(numeric_val / divisor, digits)

    for code in observe_list_codes:
        item = None
        security_type = None
        
        # 查找顺序：A股 -> 港股 -> ETF
        if code in stock_data_map:
            item = stock_data_map[code]
            security_type = 'stock'
        elif code in hk_stock_data_map:
            item = hk_stock_data_map[code]
            security_type = 'hk_stock'
        elif code in etf_data_map:
            item = etf_data_map[code]
            security_type = 'etf'
        
        if not item:
            print(f"  - Warning: Code {code} from observe list not found in any dataset.")
            continue
        
        common_info = {
            "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), 
            "trade_date": trade_date
        }

        if security_type == 'stock':
            security_info = {
                '代码': code, '名称': item.get('名称'), 
                'Price': safe_round(item.get('最新价'), 2),
                'Percent': safe_round(item.get('涨跌幅'), 2),
                'Amount': safe_round(item.get('成交额'), 2, 100_000_000),
                'PE_TTM': safe_round(item.get('市盈率-动态'), 2), 
                'PB': safe_round(item.get('市净率'), 2),
                'TotalMarketCap': safe_round(item.get('总市值'), 2, 100_000_000),
            }
        elif security_type == 'hk_stock':
            security_info = {
                '代码': code, '名称': item.get('名称'),
                'Price': safe_round(item.get('最新价'), 3), # 港股价格精度为3
                'Percent': safe_round(item.get('涨跌幅'), 2),
                'Amount': safe_round(item.get('成交额'), 2, 100_000_000),
                'PE_TTM': None, 'PB': None, 'TotalMarketCap': None, # 港股无此数据
            }
        elif security_type == 'etf':
            security_info = {
                '代码': code, '名称': item.get('名称'),
                'Price': safe_round(item.get('最新价'), 3), # ETF价格精度为3
                'Percent': safe_round(item.get('涨跌幅'), 2),
                'Amount': safe_round(item.get('成交额'), 2, 100_000_000),
                'PE_TTM': None, 'PB': None, 'TotalMarketCap': None, # ETF无此数据
            }
            
        security_info.update(common_info)
        result_list.append(security_info)
        
    print(f"Processed {len(result_list)} securities from the unified observe list.")
    return result_list


# --- 脚本执行入口 (已重构，集成新功能) ---
if __name__ == "__main__":
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    def run_and_save_task(name, func, file, *args):
        output_filepath = os.path.join(output_dir, file)
        final_data = {}; print(f"\n[{name}] -> Starting...")
        try:
            pd.set_option('mode.use_inf_as_na', True); result = func(*args); final_data = result
        except Exception as e:
            print(f"[{name}] -> Error: {e}"); final_data = {"error": str(e)}
        with open(output_filepath, 'w', encoding='utf-8') as f: json.dump(final_data, f, ensure_ascii=False, indent=4)
        print(f"[{name}] -> Finished. Data saved to {output_filepath}")

    # --- 阶段 1: 统一获取所有网络数据 ---
    print("--- Starting Data Acquisition Phase ---")
    base_trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    df_etf_raw, df_stock_raw, df_hk_stock_raw = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    try:
        df_etf_raw = ak.fund_etf_spot_em()
        print(f"Successfully fetched {len(df_etf_raw)} ETFs.")
        if not df_etf_raw.empty and '数据日期' in df_etf_raw.columns:
            ts = pd.to_datetime(df_etf_raw['数据日期'].iloc[0]); base_trade_date = ts.strftime('%Y-%m-%d'); print(f"Base trade date set to: {base_trade_date}")
    except Exception as e: print(f"Could not fetch ETF data or extract date: {e}. Using fallback date.")
    try: df_stock_raw = ak.stock_zh_a_spot_em(); print(f"Successfully fetched {len(df_stock_raw)} A-share stocks.")
    except Exception as e: print(f"Could not fetch A-share stock data: {e}")
    try: df_hk_stock_raw = ak.stock_hk_main_board_spot_em(); print(f"Successfully fetched {len(df_hk_stock_raw)} HK stocks.")
    except Exception as e: print(f"Could not fetch HK stock data: {e}")

    # --- 阶段 2: 读取本地 watchlist 文件 ---
    print("\n--- Reading Local Watchlist Files ---")
    a_share_watchlist = read_watchlist_from_json(os.path.join(output_dir, "ARHot10days_top20.json"))
    hk_share_watchlist = read_watchlist_from_json(os.path.join(output_dir, "HKHot10days_top20.json"))
    observe_list = read_watchlist_from_json(os.path.join(output_dir, "AIPEObserve.json"))

    # --- 阶段 3: 在内存中处理数据并保存结果 ---
    print("\n--- Starting Data Processing Phase ---")
    if not df_etf_raw.empty:
        run_and_save_task("ETF Report", process_etf_report, "etf_data.json", df_etf_raw, base_trade_date)
    if not df_stock_raw.empty:
        run_and_save_task("A-Share Report", process_stock_report, "stock_data.json", df_stock_raw, base_trade_date)
        run_and_save_task("A-Share Watchlist", process_stock_watchlist_report, "stock_10days_data.json", df_stock_raw, base_trade_date, a_share_watchlist)
    if not df_hk_stock_raw.empty:
        run_and_save_task("HK Stock Report", process_hk_stock_report, "hk_stock_data.json", df_hk_stock_raw, base_trade_date)
        run_and_save_task("HK Stock Watchlist", process_hk_stock_watchlist_report, "hk_stock_10days_data.json", df_hk_stock_raw, base_trade_date, hk_share_watchlist)
    
    # --- >>> 执行包含港股的统一观察列表任务 <<< ---
    if not df_stock_raw.empty or not df_etf_raw.empty or not df_hk_stock_raw.empty:
        run_and_save_task("Unified Observe List", process_observe_list_report, "stock_observe_data.json", df_stock_raw, df_etf_raw, df_hk_stock_raw, base_trade_date, observe_list)

    print("\nAll tasks finished.")
