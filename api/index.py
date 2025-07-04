# /api/index.py

from http.server import BaseHTTPRequestHandler
import json
import pandas as pd
import akshare as ak
import requests
from datetime import datetime, timezone, timedelta

# --- Helper Functions (from your Colab notebook, slightly refactored) ---

def get_beijing_time_date_str():
    """
    获取北京时间（UTC+8）当前日期，并格式化为 'YYYY-MM-DD'。
    Vercel 服务器使用 UTC 时间，需要转换为北京时间来判断交易日。
    """
    utc_now = datetime.now(timezone.utc)
    beijing_tz = timezone(timedelta(hours=8))
    beijing_now = utc_now.astimezone(beijing_tz)
    return beijing_now.strftime('%Y-%m-%d')


def get_latest_trade_date():
    """
    使用 akshare 获取最近的交易日。
    """
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
    # 如果 akshare 失败，回退到使用北京时间的当前日期
    return get_beijing_time_date_str()

def get_etf_data():
    """
    从雪球获取ETF数据。
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    # 创建一个 session 以复用 cookie
    session = requests.Session()
    # 先访问一次首页以获取必要的 cookie
    session.get("https://xueqiu.com", headers=headers)

    # 获取涨幅榜
    url_up = "https://stock.xueqiu.com/v5/stock/screener/fund/list.json?type=18&parent_type=1&order=desc&order_by=percent&page=1&size=1000"
    res_up = session.get(url_up, headers=headers)
    res_up.raise_for_status()
    data_up = res_up.json()

    # 获取跌幅榜
    url_down = "https://stock.xueqiu.com/v5/stock/screener/fund/list.json?type=18&parent_type=1&order=asc&order_by=percent&page=1&size=1000"
    res_down = session.get(url_down, headers=headers)
    res_down.raise_for_status()
    data_down = res_down.json()
    
    session.close()

    # 合并和去重
    seen_symbols = set()
    merged_list = []
    for item in data_up["data"]["list"] + data_down["data"]["list"]:
        if item['symbol'] not in seen_symbols:
            seen_symbols.add(item['symbol'])
            merged_list.append(item)
    
    # 解析数据到 DataFrame
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
            'Amount': round(item.get('amount', 0.0) / 100_000_000, 2) # 转换为亿
        }
        etf_list.append(etf_info)

    if not etf_list:
        return pd.DataFrame()
        
    return pd.DataFrame(etf_list)


def get_top_and_down_etf():
    """
    获取数据并整理成Top20涨跌幅榜。
    """
    stock_etf_data = get_etf_data()

    if stock_etf_data.empty:
        return {"error": "Failed to fetch ETF data or no data available."}

    # 按 'Percent' 降序排序获取涨幅榜
    top_20_up = stock_etf_data.sort_values(by=['Percent'], ascending=False).head(20)

    # 按 'Percent' 升序排序获取跌幅榜
    top_20_down = stock_etf_data.sort_values(by=['Percent'], ascending=True).head(20)

    # 返回结构化的字典
    return {
        "update_time_bjt": datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
        "trade_date": get_latest_trade_date(),
        "top_up_20": top_20_up.to_dict('records'),
        "top_down_20": top_20_down.to_dict('records')
    }

# --- Vercel Serverless Function Handler ---

class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.end_headers()
        
        try:
            # 获取ETF排名数据
            response_data = get_top_and_down_etf()
        except Exception as e:
            # 如果过程中出现任何异常，返回错误信息
            self.send_response(500)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            response_data = {"error": str(e)}

        # 将字典转换为JSON字符串，ensure_ascii=False 以正确显示中文
        response_body = json.dumps(response_data, ensure_ascii=False, indent=4)
        
        self.wfile.write(response_body.encode('utf-8'))
        return
