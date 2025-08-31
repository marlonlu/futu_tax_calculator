### python 环境
#### 在你的项目目录下
#### 1. 创建虚拟环境 (例如，名为 venv)
python3 -m venv venv

#### 2. 激活虚拟环境
source venv/bin/activate

#### 3. 在虚拟环境中，你可以安全地使用 pip
####    这时 `python` 和 `pip` 都指向虚拟环境内的版本
pip install -r requirements.txt

#### 4. 完成工作后，退出虚拟环境
deactivate

## 各平台数据下载流程

### 富途牛牛（Futu）
1. **API准备**：
   - 安装富途OpenD网关并启动，确保本地11111端口可用。
   - 如果需要跨网，则需要参考[富途OpenAPI文档](https://openapi.futunn.com/)设置私钥处理。
2. **下载交易订单交易成交流水**：
   - 运行 `futu/download_history_flow.py`，自动批量下载所有账户的历史订单，生成 `data/futu_history.csv`。
3. **下载港股 美股股息流水**：
   - 运行 `futu/download_cash_flow.py`，自动批量下载所有账户的历史股息及美股股息税，生成 `data/futu_cash_flow.csv`。

## 计税
**移动平均等权计算流水及年度盈利**：
   - 运行 `tax/stock_option_tax_calculator.py`，自动在税务报告文件夹下生成流水 csv，例如 `税务报告/2024_report.csv`。
    
### 支持的特性
- 查看test_data下各个用例，目前已支持股票、期权大部分场景
- 注意： 目前不支持卖空股票，如果有卖出股票，而没有买入行为，会再流水表注明需要人工核查
- 只支持 futu 平台，长桥的 api 及期权识别需要额外设置，最终生成的流水csv 如下
  - 股票代码,数量,成交价格,买卖方向,结算币种,合计手续费,交易时间
- 没玩过轮证 涡轮、期货其他产品，基于测试用例自己改代码进行验证计算

### 测试用例
在test_data下存放了验证股票、期权的测试用例场景，如果有修改计税逻辑，务必确保用例都能运行通过
该目录下所有用例都是基于移动平均等权计算