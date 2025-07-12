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

        # --- 调用 GitHub API ---
        # 你的工作流文件名，例如 main.yml
        workflow_file_name = "main.yml" 
        # 你要触发工作流的分支
        branch = "main"

        # GitHub API URL for triggering a workflow_dispatch event
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/actions/workflows/{workflow_file_name}/dispatches"

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}"
        }

        data = {
            "ref": branch,
            "inputs": {
                "trigger_source": "api_call" # 可以添加一个输入，方便在日志中区分
            }
        }
        
        try:
            # 发送 POST 请求到 GitHub API
            res = requests.post(url, headers=headers, json=data)

            # GitHub 成功接收请求后会返回 204 No Content
            if res.status_code == 204:
                self.send_response(202) # 202 Accepted 表示请求已接收，正在处理
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    "message": "Workflow triggered successfully.",
                    "details": f"Check the 'Actions' tab in your GitHub repository '{repo_owner}/{repo_name}' for progress."
                }
                self.wfile.write(json.dumps(response).encode('utf-8'))
            else:
                self.send_response(res.status_code)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    "error": "Failed to trigger GitHub workflow.",
                    "github_response": res.json()
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
        # 为了方便，可以给 GET 请求一个提示信息
        self.send_response(405) # 405 Method Not Allowed
        self.send_header('Content-type', 'application/json')
        self.send_header('Allow', 'POST')
        self.end_headers()
        response = {"error": "Method not allowed. Please use a POST request to trigger the workflow."}
        self.wfile.write(json.dumps(response).encode('utf-8'))
        return
