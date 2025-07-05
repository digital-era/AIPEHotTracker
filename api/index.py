# api/index.py

import json
import pandas as pd
import akshare as ak
import requests
import os
from datetime import datetime, timezone, timedelta

# --- 所有辅助函数 (get_beijing_time_date_str, get_latest_trade_date, get_etf_data, get_top_and_down_etf) 保持不变 ---
# (为了简洁，这里省略了这些函数的代码，请保留你原来的函数实现)
def get_beijing_time_date_str():
    # ... (代码不变)
    utc_now = datetime.now(timezone.utc)
    beijing_tz = timezone(timedelta(hours=8))
    beijing_now = utc_now.astimezone(beijing_tz)
    return beijing_now.strftime('%Y-%m-%d')

def get_latest_trade_date():
    # ... (代码不变)
    try:
        trade_date_df = ak.tool_trade_date_hist_sina()
        trade_date_df['trade_date'] = pd.to_datetime(trade_date_df['trade_date']).dt.date
        current_date = datetime.now(timezone(timedelta(hours=8))).date()
        valid_dates = trade_date_df[trade_date_df['trade_date'] <= current_date]
        if not valid_dates.empty:
            latest_trade_date = valid_dates['trade_date'].max()
            return latest_trade_date.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Error getting trade date from akshare: {e}")
    return get_beijing_time_date_str()

def get_etf_data():
    # ... (代码不变)
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'Connection': 'keep-alive',
    }
    try:
        home_url = "https://xueqiu.com"
        session.get(home_url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to access Xueqiu homepage to get cookies: {e}")
    api_headers = headers.copy()
    api_headers['Accept'] = 'application/json, text/plain, */*'
    api_headers['Host'] = 'stock.xueqiu.com'
    api_headers['Referer'] = 'https://xueqiu.com/hq'
    try:
        url_up = "https://stock.xueqiu.com/v5/stock/screener/fund/list.json?type=18&parent_type=1&order=desc&order_by=percent&page=1&size=1000"
        res_up = session.get(url_up, headers=api_headers, timeout=15)
        res_up.raise_for_status()
        data_up = res_up.json()
        url_down = "https://stock.xueqiu.com/v5/stock/screener/fund/list.json?type=18&parent_type=1&order=asc&order_by=percent&page=1&size=1000"
        res_down = session.get(url_down, headers=api_headers, timeout=15)
        res_down.raise_for_status()
        data_down = res_down.json()
    except requests.exceptions.HTTPError as e:
        response_text = e.response.text if e.response else "No response body"
        raise RuntimeError(f"HTTP Error from Xueqiu API: {e}. Response: {response_text}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Network error when requesting Xueqiu API: {e}")
    seen_symbols = set()
    merged_list = []
    if "data" in data_up and "list" in data_up["data"]:
        merged_list.extend(data_up["data"]["list"])
        seen_symbols.update(item['symbol'] for item in data_up["data"]["list"])
    if "data" in data_down and "list" in data_down["data"]:
        for item in data_down["data"]["list"]:
            if item['symbol'] not in seen_symbols:
                merged_list.append(item)
    etf_list = []
    strdate = get_latest_trade_date()
    for item in merged_list:
        etf_info = {
            '代码': item['symbol'].replace('SH', '').replace('SZ', ''),
            '名称': item['name'],
            '日期': strdate,
            'Price': round(item.get('current', 0.0), 3),
            'Percent': round(item.get('percent', 0.0), 2),
            'YC': round(item.get('current_year_percent', 0.0), 2),
            'Amount': round(item.get('amount', 0.0) / 100_000_000, 2)
        }
        etf_list.append(etf_info)
    if not etf_list:
        return pd.DataFrame()
    return pd.DataFrame(etf_list)

def get_top_and_down_etf():
    # ... (代码不变)
    stock_etf_data = get_etf_data()
    if stock_etf_data.empty:
        return {"error": "Failed to fetch ETF data or no data available."}
    top_20_up = stock_etf_data.sort_values(by=['Percent'], ascending=False).head(20)
    top_20_down = stock_etf_data.sort_values(by=['Percent'], ascending=True).head(20)
    return {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        "trade_date": get_latest_trade_date(),
        "top_up_20": top_20_up.to_dict('records'),
        "top_down_20": top_20_down.to_dict('records')
    }

# --- Main execution block ---

if __name__ == "__main__":
    # 定义输出文件的路径
    # 我们将把文件保存在仓库的根目录下的 `data` 文件夹中
    output_dir = "data"
    output_filepath = os.path.join(output_dir, "etf_data.json")

    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    print("开始获取ETF数据...")
    
    try:
        # 获取ETF排名数据
        final_data = get_top_and_down_etf()
        print("数据获取成功。")
    except Exception as e:
        # 如果过程中出现任何异常，生成一个包含错误信息的JSON
        print(f"获取数据时发生错误: {e}")
        final_data = {
            "error": str(e),
            "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
        }

    # 将最终数据写入JSON文件
    with open(output_filepath, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=4)
        
    print(f"数据已成功写入到: {output_filepath}")
