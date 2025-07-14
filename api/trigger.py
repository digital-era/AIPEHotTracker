# api/trigger.py
import os
import requests
from http.server import BaseHTTPRequestHandler
import json

class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        # --- 从 Vercel 环境变量中获取配置 ---
        token = os.environ.get('GITHUB_TOKEN')
        repo_owner = os.environ.get('GITHUB_REPO_OWNER')
        repo_name = os.environ.get('GITHUB_REPO_NAME')
        
        # 检查必要的环境变量是否存在
        if not all([token, repo_owner, repo_name]):
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
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
                self.send_response(400) # Bad Request
                self.send_header('Content-type', 'application/json')
                self.end_headers()
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
                self.send_response(202) # 202 Accepted
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    "message": "Workflow triggered successfully.",
                    "details": f"Check the 'Actions' tab in your GitHub repository '{repo_owner}/{repo_name}' for progress.",
                    "sent_inputs": workflow_inputs # 返回发送的参数，便于调试
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))
            else:
                self.send_response(res.status_code)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    "error": "Failed to trigger GitHub workflow.",
                    "github_response": res.text # 使用 res.text 获取更详细的错误信息
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))

        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {"error": f"An internal error occurred: {str(e)}"}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            
        return

    def do_GET(self):
        # 保持不变
        self.send_response(405) # 405 Method Not Allowed
        self.send_header('Content-type', 'application/json')
        self.send_header('Allow', 'POST')
        self.end_headers()
        response = {"error": "Method not allowed. Please use a POST request to trigger the workflow."}
        self.wfile.write(json.dumps(response).encode('utf-8'))
        return
