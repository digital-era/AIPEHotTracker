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
    
    # 动态导入 akshare，如果失败可以捕获
    try:
        import akshare as ak
    except ImportError:
        raise RuntimeError("akshare library is not installed. Please add it to requirements.")

    # 1. 从 akshare 获取数据
    # fund_etf_spot_em() 返回一个包含所有场内ETF实时行情的 DataFrame
    # 它本身已经包含了代码、名称、最新价、涨跌幅等所有需要的信息
    try:
        df_raw = ak.fund_etf_spot_em()
        print(f"Successfully fetched {len(df_raw)} ETFs from akshare.")
    except Exception as e:
        # 如果 akshare 调用失败，抛出错误
        raise RuntimeError(f"Failed to fetch data from akshare.fund_etf_spot_em(). Error: {e}")

    # 2. 数据清洗和重命名
    # akshare 返回的列名是中文的，我们需要将其转换为我们期望的英文 key
    # '代码', '名称', '最新价', '涨跌额', '涨跌幅', '成交量', '成交额', '开盘价', '最高价', '最低价', '昨收'
    
    # 筛选我们需要的列
    required_columns = ['代码', '名称', '最新价', '涨跌幅', '成交额']
    df_selected = df_raw[required_columns].copy()

    # 重命名列以匹配我们之前的 JSON 结构
    df_selected.rename(columns={
        '代码': '代码',         # 保持不变
        '名称': '名称',         # 保持不变
        '最新价': 'Price',
        '涨跌幅': 'Percent',
        '成交额': 'Amount'
    }, inplace=True)

    # 3. 数据类型转换和格式化
    # 确保数据类型正确，处理可能存在的 '-' 等非数字值
    # pd.to_numeric 会将无法转换的值变为 NaN
    df_selected['Price'] = pd.to_numeric(df_selected['Price'], errors='coerce')
    df_selected['Percent'] = pd.to_numeric(df_selected['Percent'], errors='coerce')
    df_selected['Amount'] = pd.to_numeric(df_selected['Amount'], errors='coerce')

    # 删除包含任何 NaN 值的行，确保数据质量
    df_selected.dropna(inplace=True)

    # 格式化数据：成交额转换为“亿”
    df_selected['Amount'] = (df_selected['Amount'] / 100_000_000).round(2)
    df_selected['Price'] = df_selected['Price'].round(3)
    df_selected['Percent'] = df_selected['Percent'].round(2)

    # 4. 生成 Top 20 榜单
    if df_selected.empty:
        print("No valid ETF data after cleaning. Exiting.")
        return {"error": "No valid ETF data available from akshare after cleaning."}

    # 按 'Percent' 降序排序获取涨幅榜 Top 20
    df_top_up = df_selected.sort_values(by='Percent', ascending=False).head(20)
    print("\n--- Top 20 Gainers ---")
    print(df_top_up.to_string(index=False))

    # 按 'Percent' 升序排序获取跌幅榜 Top 20
    df_top_down = df_selected.sort_values(by='Percent', ascending=True).head(20)
    print("\n--- Top 20 Losers ---")
    print(df_top_down.to_string(index=False))

    # 5. 构建最终的报告字典
    report = {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        "trade_date": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d'), # 使用当前日期作为交易日
        "top_up_20": df_top_up.to_dict('records'),
        "top_down_20": df_top_down.to_dict('records')
    }
    
    return report

# --- 2. 脚本执行入口 ---

if __name__ == "__main__":
    output_dir = "data"
    output_filepath = os.path.join(output_dir, "etf_data.json")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    final_data = {}
    try:
        # 调用新的核心函数
        final_data = get_etf_report_from_akshare()
    except Exception as e:
        # 统一的异常捕获
        print(f"\nAn critical error occurred during script execution: {e}")
        final_data = {
            "error": str(e),
            "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        }

    # 将最终数据写入 JSON 文件
    print(f"\nWriting data to {output_filepath}...")
    with open(output_filepath, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
        
    print("Script finished successfully.")
