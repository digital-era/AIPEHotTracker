# api/trigger.py
import os
import requests
from http.server import BaseHTTPRequestHandler
import json

class handler(BaseHTTPRequestHandler):

    # 定义允许的来源 (Origin)
    # 这是您的前端页面部署的域名。
    # 如果您的前端可能部署在不同的地方，您需要：
    # 1. 将此值作为 Vercel 环境变量来配置。
    # 2. 或者，如果允许所有来源（不推荐用于生产环境），将其设置为 "*"。
    # 根据您提供的错误信息，您的前端是 'https://digital-era.github.io'
    ALLOWED_ORIGIN = "https://digital-era.github.io"

    def _set_headers(self, status_code=200, content_type='application/json'):
        """
        设置HTTP响应头部，包括CORS相关头部。
        """
        self.send_response(status_code)
        self.send_header('Content-type', content_type)
        
        # --- CORS Headers ---
        # 允许指定来源访问此资源
        self.send_header('Access-Control-Allow-Origin', self.ALLOWED_ORIGIN)
        # 允许的HTTP方法 (POST 和 OPTIONS 是必须的，因为您的前端发POST请求，浏览器会先发OPTIONS预检)
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        # 允许的请求头部 (Content-Type 是必须的，因为您的前端发送的是 JSON)
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        # 允许凭证（如果您的前端请求包含 cookie、HTTP 认证或客户端 SSL 证书，需要此项）
        # 这里您的前端似乎没有，所以可以省略或设置为 'false'
        # self.send_header('Access-Control-Allow-Credentials', 'true') 
        # 预检请求的有效期，单位秒 (缓存预检结果，减少后续同源请求的预检次数)
        self.send_header('Access-Control-Max-Age', '86400') # 24小时
        
        self.end_headers()

    def do_OPTIONS(self):
        """
        处理 CORS 预检请求。
        浏览器在发送复杂的跨域请求（如 POST 请求）之前会发送 OPTIONS 请求。
        """
        self._set_headers(200) # 预检请求成功，返回 200 OK
        self.wfile.write(b'') # 预检请求的响应体通常为空

    def do_POST(self):
        """
        处理 POST 请求，触发 GitHub Workflow。
        """
        # 在处理请求之前先设置 CORS 头部
        # 如果后续发生错误，也能确保 CORS 头部被发送
        
        # --- 从 Vercel 环境变量中获取配置 ---
        token = os.environ.get('GITHUB_TOKEN')
        repo_owner = os.environ.get('GITHUB_REPO_OWNER')
        repo_name = os.environ.get('GITHUB_REPO_NAME')
        
        # 检查必要的环境变量是否存在
        if not all([token, repo_owner, repo_name]):
            self._set_headers(500) # 设置错误响应的头部
            response = {"error": "Server configuration is incomplete. Required environment variables are missing."}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            return

        # --- 解析传入的POST请求体 ---
        content_length = int(self.headers.get('Content-Length', 0))
        post_data_raw = self.rfile.read(content_length)
        post_data = {}
        if post_data_raw:
            try:
                post_data = json.loads(post_data_raw)
            except json.JSONDecodeError:
                self._set_headers(400) # Bad Request
                response = {"error": "Invalid JSON format in request body."}
                self.wfile.write(json.dumps(response).encode('utf-8'))
                return

        # --- 调用 GitHub API ---
        workflow_file_name = "main.yml" 
        branch = "main"
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_file_name}/dispatches"

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}"
        }

        # --- 构建包含动态参数的 inputs ---
        # 基础 inputs
        workflow_inputs = {
            "trigger_source": "api_call"
        }
        
        # 检查并添加 dynamiclist (A股) 和 dynamicHKlist (H股)
        # GitHub Actions 的 inputs 只接受字符串，所以我们将列表转换为 JSON 字符串
        dynamic_list_a = post_data.get('dynamiclist')
        if dynamic_list_a and isinstance(dynamic_list_a, list):
            workflow_inputs['dynamiclist'] = json.dumps(dynamic_list_a)

        dynamic_list_hk = post_data.get('dynamicHKlist')
        if dynamic_list_hk and isinstance(dynamic_list_hk, list):
            workflow_inputs['dynamicHKlist'] = json.dumps(dynamic_list_hk)

        data = {
            "ref": branch,
            "inputs": workflow_inputs
        }
        
        try:
            # 发送 POST 请求到 GitHub API
            res = requests.post(url, headers=headers, json=data)

            # GitHub 成功接收请求后会返回 204 No Content
            if res.status_code == 204:
                self._set_headers(202) # 202 Accepted
                response = {
                    "message": "Workflow triggered successfully.",
                    "details": f"Check the 'Actions' tab in your GitHub repository '{repo_owner}/{repo_name}' for progress.",
                    "sent_inputs": workflow_inputs # 返回发送的参数，便于调试
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))
            else:
                self._set_headers(res.status_code)
                response = {
                    "error": "Failed to trigger GitHub workflow.",
                    "github_response": res.text # 使用 res.text 获取更详细的错误信息
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))

        except Exception as e:
            self._set_headers(500)
            response = {"error": f"An internal error occurred: {str(e)}"}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            
        return

    def do_GET(self):
        # 尽管不允许 GET 请求，但为了遵循 CORS 策略，依然需要发送正确的 CORS 头部
        self._set_headers(405) # 405 Method Not Allowed
        # 对于 405 响应，通常也会通过 Allow 头部告知客户端允许的方法
        self.send_header('Allow', 'POST, OPTIONS') # 告知客户端允许 POST 和 OPTIONS 方法
        self.end_headers() # 确保在写入响应体之前结束头部
        response = {"error": "Method not allowed. Please use a POST request to trigger the workflow."}
        self.wfile.write(json.dumps(response).encode('utf-8'))
        return
