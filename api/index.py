# api/index.py
# 最终版：保留所有旧场景，并新增处理动态参数的场景 (已修复键名问题)

import os
import json
import pandas as pd
import akshare as ak
from datetime import datetime, timezone, timedelta

# --- 辅助函数 ---
def read_watchlist_from_json(file_path):
    # ... (代码不变)
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

# --- 辅助函数 (已修正) ---
def read_flow_info_base(file_path):
    """
    读取 FlowInfoBase.json 并将其转换为以股票代码为键的字典以便快速查找。
    """
    print(f"Reading flow info from: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # <-- [修正] 将 'code' 修改为 '代码'，以匹配JSON文件中的实际键名 -->
        flow_map = {str(item['代码']): item for item in data}
        print(f"Successfully created a lookup map for {len(flow_map)} codes from flow info.")
        return flow_map
    except FileNotFoundError:
        print(f"Warning: Flow info file not found: {file_path}. Extra fields will be empty.")
        return {}
    except Exception as e:
        print(f"Error reading or processing flow info file {file_path}: {e}")
        # 如果出错（比如键名错误），返回空字典，避免程序崩溃
        return {}

# --- 原有处理函数 (保持不变) ---
def process_etf_report(df_raw, trade_date):
    # ... (代码不变)
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
    # ... (代码不变)
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
    # ... (代码不变)
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
    # ... (代码不变)
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
    # ... (代码不变)
    print("\n--- (5/x) Processing Hong Kong Stock Watchlist Data ---")
    if not watchlist_codes: return {"error": "Watchlist is empty, skipping."}
    df_raw['代码'] = df_raw['代码'].astype(str); live_data_map = df_raw.set_index('代码').to_dict('index'); result_list = []
    for code in watchlist_codes:
        if code in live_data_map:
            item = live_data_map[code]
            stock_info = {'代码': code, '名称': item.get('名称'), 'Price': round(pd.to_numeric(item.get('最新价'), errors='coerce'), 3), 'Percent': round(pd.to_numeric(item.get('涨跌幅'), errors='coerce'), 2), 'Amount': round(pd.to_numeric(item.get('成交额'), errors='coerce') / 100_000_000, 2), "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), "trade_date": trade_date}
            result_list.append(stock_info)
    print(f"Processed {len(result_list)} stocks from the HK Stock watchlist."); return result_list
    
def process_observe_list_report(df_stock_raw, df_etf_raw, df_hk_stock_raw, trade_date, observe_list_codes):
    # ... (代码不变)
    print("\n--- Processing Unified Observe List (A-Share, HK-Stock, ETF) ---")
    
    if not observe_list_codes:
        print("Observe list is empty, skipping.")
        return {"error": "Observe list is empty, skipping."}

    stock_data_map = {}
    if not df_stock_raw.empty:
        df_stock_raw['代码'] = df_stock_raw['代码'].astype(str)
        stock_data_map = df_stock_raw.set_index('代码').to_dict('index')

    etf_data_map = {}
    if not df_etf_raw.empty:
        df_etf_raw['代码'] = df_etf_raw['代码'].astype(str)
        etf_data_map = df_etf_raw.set_index('代码').to_dict('index')

    hk_stock_data_map = {}
    if not df_hk_stock_raw.empty:
        df_hk_stock_raw['代码'] = df_hk_stock_raw['代码'].astype(str)
        hk_stock_data_map = df_hk_stock_raw.set_index('代码').to_dict('index')

    result_list = []
    
    def safe_round(value, digits, divisor=1):
        numeric_val = pd.to_numeric(value, errors='coerce')
        if pd.isna(numeric_val): return None
        return round(numeric_val / divisor, digits)

    for code in observe_list_codes:
        item, security_type = None, None
        if code in stock_data_map: item, security_type = stock_data_map[code], 'stock'
        elif code in hk_stock_data_map: item, security_type = hk_stock_data_map[code], 'hk_stock'
        elif code in etf_data_map: item, security_type = etf_data_map[code], 'etf'
        
        if not item:
            print(f"  - Warning: Code {code} from observe list not found in any dataset.")
            continue
        
        common_info = {
            "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'), 
            "trade_date": trade_date
        }

        if security_type == 'stock':
            security_info = {'代码': code, '名称': item.get('名称'), 'Price': safe_round(item.get('最新价'), 2), 'Percent': safe_round(item.get('涨跌幅'), 2), 'Amount': safe_round(item.get('成交额'), 2, 100_000_000), 'PE_TTM': safe_round(item.get('市盈率-动态'), 2), 'PB': safe_round(item.get('市净率'), 2), 'TotalMarketCap': safe_round(item.get('总市值'), 2, 100_000_000)}
        elif security_type == 'hk_stock':
            security_info = {'代码': code, '名称': item.get('名称'), 'Price': safe_round(item.get('最新价'), 3), 'Percent': safe_round(item.get('涨跌幅'), 2), 'Amount': safe_round(item.get('成交额'), 2, 100_000_000), 'PE_TTM': None, 'PB': None, 'TotalMarketCap': None}
        elif security_type == 'etf':
            security_info = {'代码': code, '名称': item.get('名称'), 'Price': safe_round(item.get('最新价'), 3), 'Percent': safe_round(item.get('涨跌幅'), 2), 'Amount': safe_round(item.get('成交额'), 2, 100_000_000), 'PE_TTM': None, 'PB': None, 'TotalMarketCap': None}
            
        security_info.update(common_info)
        result_list.append(security_info)
        
    print(f"Processed {len(result_list)} securities from the list provided.")
    if not result_list and not stock_data_map and not hk_stock_data_map and not etf_data_map:
        return {"error": "All underlying data sources failed to fetch data."}
        
    return result_list
    
def process_dynamic_a_share_report(df_stock_raw, trade_date, dynamic_codes, flow_info_map):
    # ... (代码不变)
    """
    处理动态传入的A股列表，并从 flow_info_map 中补充额外字段。
    """
    print("\n--- (NEW DYNAMIC TASK) Processing Dynamic A-Share List with Flow Info ---")
    if not dynamic_codes:
        return {"error": "Dynamic A-share list is empty."}
    
    base_results = process_observe_list_report(df_stock_raw, pd.DataFrame(), pd.DataFrame(), trade_date, dynamic_codes)
    
    if "error" in base_results or not base_results:
        return base_results

    extra_fields = ['PotScore', '总净流入占比_5日总和', '主力净流入-净占比', 'l2name', 'Price20-day-MA_IsUp']
    
    enriched_results = []
    for stock_item in base_results:
        code = stock_item['代码']
        flow_data = flow_info_map.get(code, {})
        
        for field in extra_fields:
            stock_item[field] = flow_data.get(field)
            
        enriched_results.append(stock_item)
        
    print(f"Enriched {len(enriched_results)} stocks with flow information.")
    return enriched_results

# --- 脚本执行入口 (保持不变) ---
if __name__ == "__main__":
    # ... (代码不变)
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    def run_and_save_task(name, func, file, *args):
        output_filepath = os.path.join(output_dir, file)
        final_data = {}
        print(f"\n[{name}] -> Starting...")
        try:
            pd.set_option('mode.use_inf_as_na', True)
            result = func(*args)
            final_data = result
        except Exception as e:
            print(f"[{name}] -> Error: {e}")
            import traceback
            traceback.print_exc()
            final_data = {"error": str(e)}
        with open(output_filepath, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, ensure_ascii=False, indent=4)
        print(f"[{name}] -> Finished. Data saved to {output_filepath}")

    print("--- Starting Data Acquisition Phase ---")
    base_trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    df_etf_raw, df_stock_raw, df_hk_stock_raw = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    try:
        df_etf_raw = ak.fund_etf_spot_em()
        print(f"Successfully fetched {len(df_etf_raw)} ETFs.")
        if not df_etf_raw.empty and '数据日期' in df_etf_raw.columns and pd.to_datetime(df_etf_raw['数据日期'].iloc[0], errors='coerce') is not pd.NaT:
            ts = pd.to_datetime(df_etf_raw['数据日期'].iloc[0])
            base_trade_date = ts.strftime('%Y-%m-%d')
            print(f"Base trade date set to: {base_trade_date}")
    except Exception as e: print(f"Could not fetch ETF data or extract date: {e}. Using fallback date.")
    try:
        df_stock_raw = ak.stock_zh_a_spot_em()
        print(f"Successfully fetched {len(df_stock_raw)} A-share stocks.")
    except Exception as e: print(f"Could not fetch A-share stock data: {e}")
    try:
        df_hk_stock_raw = ak.stock_hk_main_board_spot_em()
        print(f"Successfully fetched {len(df_hk_stock_raw)} HK stocks.")
    except Exception as e: print(f"Could not fetch HK stock data: {e}")

    print("\n--- Reading Local Files & Dynamic Inputs ---")
    
    flow_info_map = read_flow_info_base(os.path.join(output_dir, "FlowInfoBase.json"))

    a_share_watchlist = read_watchlist_from_json(os.path.join(output_dir, "ARHot10days_top20.json"))
    hk_share_watchlist = read_watchlist_from_json(os.path.join(output_dir, "HKHot10days_top20.json"))
    observe_list = read_watchlist_from_json(os.path.join(output_dir, "AIPEObserve.json"))

    dynamic_a_list, dynamic_hk_list = [], []
    dynamic_a_list_str = os.environ.get('INPUT_DYNAMICLIST')
    if dynamic_a_list_str:
        try:
            dynamic_a_list = json.loads(dynamic_a_list_str)
            print(f"Found and parsed {len(dynamic_a_list)} codes from dynamic A-share list input.")
        except json.JSONDecodeError:
            print(f"Error: Could not parse dynamic A-share list input: '{dynamic_a_list_str}'.")
    
    dynamic_hk_list_str = os.environ.get('INPUT_DYNAMICHKLIST')
    if dynamic_hk_list_str:
        try:
            dynamic_hk_list = json.loads(dynamic_hk_list_str)
            print(f"Found and parsed {len(dynamic_hk_list)} codes from dynamic HK-share list input.")
        except json.JSONDecodeError:
            print(f"Error: Could not parse dynamic HK-share list input: '{dynamic_hk_list_str}'.")

    print("\n--- Starting Data Processing Phase ---")
    
    if not df_etf_raw.empty:
        run_and_save_task("ETF Report", process_etf_report, "etf_data.json", df_etf_raw, base_trade_date)
    if not df_stock_raw.empty:
        run_and_save_task("A-Share Report", process_stock_report, "stock_data.json", df_stock_raw, base_trade_date)
        run_and_save_task("A-Share Watchlist", process_stock_watchlist_report, "stock_10days_data.json", df_stock_raw, base_trade_date, a_share_watchlist)
    if not df_hk_stock_raw.empty:
        run_and_save_task("HK Stock Report", process_hk_stock_report, "hk_stock_data.json", df_hk_stock_raw, base_trade_date)
        run_and_save_task("HK Stock Watchlist", process_hk_stock_watchlist_report, "hk_stock_10days_data.json", df_hk_stock_raw, base_trade_date, hk_share_watchlist)
    
    if not df_stock_raw.empty or not df_etf_raw.empty or not df_hk_stock_raw.empty:
        run_and_save_task("Unified Observe List", process_observe_list_report, "stock_observe_data.json", df_stock_raw, df_etf_raw, df_hk_stock_raw, base_trade_date, observe_list)
        
    if dynamic_a_list and not df_stock_raw.empty:
        run_and_save_task(
            "Dynamic A-Share List", 
            process_dynamic_a_share_report, 
            "stock_dynamic_data.json", 
            df_stock_raw, 
            base_trade_date, 
            dynamic_a_list, 
            flow_info_map
        )
        
    if dynamic_hk_list and not df_hk_stock_raw.empty:
        run_and_save_task(
            "Dynamic HK-Share List",
            process_observe_list_report,
            "hk_stock_dynamic_data.json",
            pd.DataFrame(),
            pd.DataFrame(),
            df_hk_stock_raw,
            base_trade_date,
            dynamic_hk_list
        )

    print("\nAll tasks finished.")
