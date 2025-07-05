# api/index.py

import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

# --- 1. 辅助函数：获取日期 ---

def get_latest_trade_date():
    """
    使用 akshare 获取最近的交易日。如果失败，则返回北京时间的当前日期。
    """
    try:
        # 注意：akshare 可能会在 GitHub Actions 环境中安装或运行缓慢/失败
        # 如果 akshare 经常出问题，可以考虑直接使用 get_beijing_time_date_str
        import akshare as ak
        trade_date_df = ak.tool_trade_date_hist_sina()
        # akshare 返回的日期可能是 '2024-05-31' 或 20240531，需要兼容处理
        trade_date_df['trade_date'] = pd.to_datetime(trade_date_df['trade_date']).dt.date
        
        beijing_now = datetime.now(timezone(timedelta(hours=8)))
        current_date = beijing_now.date()
        
        valid_dates = trade_date_df[trade_date_df['trade_date'] <= current_date]
        if not valid_dates.empty:
            latest_trade_date = valid_dates['trade_date'].max()
            return latest_trade_date.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Warning: Failed to get trade date from akshare: {e}. Falling back to current date.")
    
    # Fallback function
    beijing_now = datetime.now(timezone(timedelta(hours=8)))
    return beijing_now.strftime('%Y-%m-%d')


# --- 2. 核心函数：与雪球 API 交互 ---

def fetch_xueqiu_data_safely(session, url, headers):
    """
    使用提供的 session 和 headers 安全地请求雪球 API。
    """
    try:
        response = session.get(url, headers=headers, timeout=15)
        response.raise_for_status()  # 如果状态码不是 2xx，则抛出 HTTPError
        return response.json()
    except requests.exceptions.HTTPError as e:
        # 打印详细错误，帮助调试 4xx 或 5xx 问题
        print(f"HTTP Error fetching {url}: {e}")
        print(f"Response Body: {e.response.text}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Network error fetching {url}: {e}")
        raise

def get_all_etf_data():
    """
    获取所有ETF的涨跌幅数据，并合并去重。
    这是解决 400 错误的关键部分。
    """
    # 使用 Session 对象自动管理和发送 Cookie
    with requests.Session() as session:
        # 1. 准备通用的浏览器 Headers
        base_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        }

        # 2. 关键步骤：先访问雪球首页，让 session 获取必要的 Cookie
        # 这一步模拟了浏览器首次打开雪球网站的行为，对于无头环境（如 GitHub Actions）至关重要
        try:
            print("Accessing Xueqiu homepage to acquire cookies...")
            home_url = "https://xueqiu.com/"
            session.get(home_url, headers=base_headers, timeout=10)
            print("Cookies acquired successfully.")
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Fatal: Could not access Xueqiu homepage. Error: {e}")

        # 3. 准备用于 API 请求的 Headers
        api_headers = base_headers.copy()
        api_headers['Host'] = 'stock.xueqiu.com'
        api_headers['Referer'] = 'https://xueqiu.com/hq'

        # 4. 定义要请求的 API 地址
        url_up = "https://stock.xueqiu.com/v5/stock/screener/fund/list.json?type=18&parent_type=1&order=desc&order_by=percent&page=1&size=1000"
        url_down = "https://stock.xueqiu.com/v5/stock/screener/fund/list.json?type=18&parent_type=1&order=asc&order_by=percent&page=1&size=1000"
        
        # 5. 分别获取涨幅榜和跌幅榜
        print("Fetching ETF gainers list...")
        json_data_up = fetch_xueqiu_data_safely(session, url_up, api_headers)
        
        print("Fetching ETF losers list...")
        json_data_down = fetch_xueqiu_data_safely(session, url_down, api_headers)

        # 6. 合并和去重数据
        # 参照你的原始逻辑，将涨幅榜和跌幅榜合并，并根据 symbol 去重
        seen_symbols = set()
        merged_list = []

        if json_data_up and "data" in json_data_up and "list" in json_data_up["data"]:
            for item in json_data_up["data"]["list"]:
                if item['symbol'] not in seen_symbols:
                    merged_list.append(item)
                    seen_symbols.add(item['symbol'])
        
        if json_data_down and "data" in json_data_down and "list" in json_data_down["data"]:
            for item in json_data_down["data"]["list"]:
                if item['symbol'] not in seen_symbols:
                    merged_list.append(item)
                    seen_symbols.add(item['symbol'])
        
        print(f"Total unique ETFs fetched: {len(merged_list)}")
        return merged_list

def process_etf_data(etf_raw_list):
    """
    将从 API 获取的原始列表数据转换为结构化的 DataFrame。
    """
    if not etf_raw_list:
        return pd.DataFrame()

    processed_list = []
    for item in etf_raw_list:
        # 过滤掉非沪深市场的ETF（如果有）
        if not (item['symbol'].startswith('SH') or item['symbol'].startswith('SZ')):
            continue

        etf_info = {
            '代码': item['symbol'][2:],  # 去掉 SH/SZ 前缀
            '名称': item['name'],
            'Price': round(item.get('current', 0.0), 3),
            'Percent': round(item.get('percent', 0.0), 2),
            'YC': round(item.get('current_year_percent', 0.0), 2), # 年初至今涨跌幅
            'Amount': round(item.get('amount', 0.0) / 100_000_000, 2) if item.get('amount') else 0.0, # 转换为亿
        }
        processed_list.append(etf_info)
    
    return pd.DataFrame(processed_list)


# --- 3. 主逻辑：获取、处理并生成最终 JSON ---

def generate_etf_report():
    """
    主函数，整合所有步骤，生成最终的报告字典。
    """
    print("Starting ETF Top 20 up and down analysis...")
    
    # 获取所有 ETF 数据
    all_etf_raw_data = get_all_etf_data()
    
    # 将原始数据处理成 DataFrame
    df_etf = process_etf_data(all_etf_raw_data)

    if df_etf.empty:
        print("No ETF data to process. Exiting.")
        return {"error": "Failed to fetch or process any ETF data."}
    
    # 按 'Percent' 降序排序获取涨幅榜 Top 20
    df_top_up = df_etf.sort_values(by=['Percent'], ascending=False).head(20)
    print("\n--- Top 20 Gainers ---")
    print(df_top_up.to_string(index=False))

    # 按 'Percent' 升序排序获取跌幅榜 Top 20 (使用 tail 在已降序的 df 上取最后20个)
    # 为了逻辑清晰，我们还是重新排序
    df_top_down = df_etf.sort_values(by=['Percent'], ascending=True).head(20)
    print("\n--- Top 20 Losers ---")
    print(df_top_down.to_string(index=False))
    
    # 准备最终的 JSON 输出结构
    report = {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        "trade_date": get_latest_trade_date(),
        "top_up_20": df_top_up.to_dict('records'),
        "top_down_20": df_top_down.to_dict('records')
    }
    
    return report


# --- 4. 脚本执行入口 ---

if __name__ == "__main__":
    output_dir = "data"
    output_filepath = os.path.join(output_dir, "etf_data.json")

    os.makedirs(output_dir, exist_ok=True)
    
    final_data = {}
    try:
        final_data = generate_etf_report()
    except Exception as e:
        print(f"\nAn error occurred during the script execution: {e}")
        # 即使失败，也生成包含错误信息的 JSON
        final_data = {
            "error": str(e),
            "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        }

    # 将最终数据写入 JSON 文件
    print(f"\nWriting data to {output_filepath}...")
    with open(output_filepath, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
        
    print("Script finished successfully.")
