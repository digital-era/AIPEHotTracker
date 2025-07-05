# api/index.py

import os
import json
import pandas as pd
from datetime import datetime, timezone, timedelta

# --- 1. 核心函数：使用 akshare 获取并处理数据 ---

def get_etf_report_from_akshare():
    """
    使用 akshare.fund_etf_spot_em() 获取实时ETF数据，
    并整理成包含涨跌幅 Top 20 的报告。
    """
    print("Attempting to fetch ETF data using akshare.fund_etf_spot_em...")
    
    try:
        import akshare as ak
    except ImportError:
        raise RuntimeError("akshare library is not installed. Please add it to requirements.")

    # 1. 从 akshare 获取数据
    try:
        df_raw = ak.fund_etf_spot_em()
        print(f"Successfully fetched {len(df_raw)} ETFs from akshare.")
    except Exception as e:
        raise RuntimeError(f"Failed to fetch data from akshare.fund_etf_spot_em(). Error: {e}")

    # 检查返回的数据是否为空，如果为空则无法继续
    if df_raw.empty:
        raise RuntimeError("akshare returned an empty DataFrame. Cannot proceed.")

    # --- 关键修改：从数据源中提取交易日期 ---
    # akshare 返回的数据中，所有行的 '数据日期' 都是相同的，代表这份快照的日期。
    # 我们从第一行获取即可。
    try:
        trade_date = df_raw['数据日期'].iloc[0]
        print(f"Extracted trade date from data source: {trade_date}")
    except (KeyError, IndexError):
        # 如果 '数据日期' 列不存在或 DataFrame 为空，则使用当前日期作为备用
        print("Warning: '数据日期' column not found. Falling back to current Beijing time date.")
        trade_date = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')
    # ----------------------------------------

    # 2. 数据清洗和重命名
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']
    df_selected = df_raw[required_columns].copy()

    df_selected.rename(columns={
        '最新价': 'Price',
        '涨跌幅': 'Percent',
        '成交额': 'Amount'
    }, inplace=True)

    # 3. 数据类型转换和格式化
    df_selected['Price'] = pd.to_numeric(df_selected['Price'], errors='coerce')
    df_selected['Percent'] = pd.to_numeric(df_selected['Percent'], errors='coerce')
    df_selected['Amount'] = pd.to_numeric(df_selected['Amount'], errors='coerce')
    df_selected.dropna(inplace=True)

    df_selected['Amount'] = (df_selected['Amount'] / 100_000_000).round(2)
    df_selected['Price'] = df_selected['Price'].round(3)
    df_selected['Percent'] = df_selected['Percent'].round(2)

    # 4. 生成 Top 20 榜单
    if df_selected.empty:
        print("No valid ETF data after cleaning. Exiting.")
        return {"error": "No valid ETF data available from akshare after cleaning."}

    df_top_up = df_selected.sort_values(by='Percent', ascending=False).head(20)
    print("\n--- Top 20 Gainers ---")
    print(df_top_up.to_string(index=False))

    df_top_down = df_selected.sort_values(by='Percent', ascending=True).head(20)
    print("\n--- Top 20 Losers ---")
    print(df_top_down.to_string(index=False))

    # 5. 构建最终的报告字典
    report = {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        # --- 关键修改：使用从数据中提取的 trade_date ---
        "trade_date": trade_date,
        # ---------------------------------------------
        "top_up_20": df_top_up.to_dict('records'),
        "top_down_20": df_top_down.to_dict('records')
    }
    
    return report

# --- 2. 脚本执行入口 (保持不变) ---

if __name__ == "__main__":
    output_dir = "data"
    output_filepath = os.path.join(output_dir, "etf_data.json")

    os.makedirs(output_dir, exist_ok=True)
    
    final_data = {}
    try:
        final_data = get_etf_report_from_akshare()
    except Exception as e:
        print(f"\nAn critical error occurred during script execution: {e}")
        final_data = {
            "error": str(e),
            "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        }

    print(f"\nWriting data to {output_filepath}...")
    with open(output_filepath, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
        
    print("Script finished successfully.")
