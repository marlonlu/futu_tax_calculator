## python 环境
# 在你的项目目录下
# 1. 创建虚拟环境 (例如，名为 venv)
python3 -m venv venv

# 2. 激活虚拟环境
source venv/bin/activate

# 3. 在虚拟环境中，你可以安全地使用 pip
#    这时 `python` 和 `pip` 都指向虚拟环境内的版本
pip install -r requirements.txt

# 4. 完成工作后，退出虚拟环境
deactivate

## 各平台数据下载流程

### 富途牛牛（Futu）
1. **API准备**：
   - 安装富途OpenD网关并启动，确保本地11111端口可用。
   - 参考[富途OpenAPI文档](https://openapi.futunn.com/)获取API密钥。
2. **下载交易订单交易成交流水**：
   - 运行 `futu/download_history_flow.py`，自动批量下载所有账户的历史订单，生成 `data/futu_history.csv`。
3. **下载港股 美股股息流水**：
   - 运行 `futu/download_cash_flow.py`，自动批量下载所有账户的历史股息及美股股息税，生成 `data/futu_cash_flow.csv`。