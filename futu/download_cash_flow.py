import os

from dotenv import load_dotenv
from futu import *
import pandas as pd
from datetime import datetime, timedelta
import time
from collections import deque
from threading import Lock

class RateLimiter:
    def __init__(self, max_requests, time_window):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
        self.lock = Lock()
    
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            # 移除过期的请求记录
            while self.requests and now - self.requests[0] > self.time_window:
                self.requests.popleft()
            
            # 如果达到最大请求数，等待
            if len(self.requests) >= self.max_requests:
                wait_time = self.requests[0] + self.time_window - now
                if wait_time > 0:
                    time.sleep(wait_time)
            
            # 添加新的请求记录
            self.requests.append(time.time())

def get_history_cash_flow():
    SysConfig.INIT_RSA_FILE = os.environ.get("FUTU_RSA")
    host = os.environ.get("FUTU_ADDRESS")
    port = int(os.environ.get("FUTU_PORT"))
    # 创建OpenD连接
    quote_ctx = OpenQuoteContext(host=host, port=port)
    # 不指定市场，获取所有市场的交易权限
    trade_ctx = OpenSecTradeContext(host=host, port=port, filter_trdmarket=TrdMarket.NONE, is_encrypt=True)
    
    # 创建请求限制器（30秒内最多20次请求）
    rate_limiter = RateLimiter(max_requests=20, time_window=30)
    
    # 存储所有账户的所有订单
    all_accounts_orders = []
    
    # 定义要查询的市场列表
    # markets_to_query = [TrdMarket.US, TrdMarket.HK]
    markets_to_query = [TrdMarket.NONE]
    # card_num '1001100120228969' 老的港股，'1001100520109503' 老的美股， '1001378017386807' 新的保证金综合账户
    card_num_to_query = ['1001100120228969', '1001100520109503']
    try:
        # 获取账户列表
        ret, acc_list_df = trade_ctx.get_acc_list()
        if ret != RET_OK or not isinstance(acc_list_df, pd.DataFrame):
            print(f'获取账户列表失败: {acc_list_df}')
            return
        
        # 遍历所有账户
        for _, acc_row in acc_list_df.iterrows():
            acc_id = acc_row.get('acc_id')
            card_num = acc_row.get('card_num')
            if acc_row.get("trd_env")==TrdEnv.SIMULATE:
                continue
            if acc_id is None:
                continue
            if card_num not in card_num_to_query:
                continue
            print(acc_row.get("uni_card_num"))
            
            try:
                acc_id = int(acc_id)
            except (ValueError, TypeError):
                print(f"无效的账户ID: {acc_id}")
                continue

            print(f"开始处理账户acc_id: {acc_row.get('acc_id')}")
            print(f"开始处理账户card_num: {acc_row.get('card_num')}")

            # 遍历所有市场
            for market in markets_to_query:
                # print(f"  ...正在查询市场: {market}")
                
                # 设置查询时间范围
                start_date = datetime(2022, 1, 1)
                end_date = datetime(2025, 1, 1)
                
                # 每3个月为一个批次
                current_start = start_date
                
                while current_start < end_date:
                    # 计算当前批次的结束时间
                    current_end = min(current_start + timedelta(days=1), end_date)
                    
                    print(f"正在获取 {current_start.strftime('%Y-%m-%d %H:%M:%S')} 到 {current_end.strftime('%Y-%m-%d %H:%M:%S')} 的订单数据...")
                    
                    # 等待请求限制
                    rate_limiter.wait_if_needed()

                    ret, data = trade_ctx.get_acc_cash_flow(
                        clearing_date=current_start.strftime('%Y-%m-%d'),
                        acc_id=acc_id,
                    )
                    if ret == RET_OK:
                        print(data)

                    if isinstance(data, pd.DataFrame) and not data.empty:
                        # 使用布尔索引来筛选DataFrame中的行
                        # 条件1: 货币是USD
                        condition_currency_hkd = data['currency'] == 'HKD'
                        condition_currency_usd = data['currency'] == 'USD'

                        # 条件2: cashflow_amount 不为0
                        condition_amount = data['cashflow_amount'] != 0

                        # 条件3: 判断是否股息 以下条件任意为
                        # 美股 判断 cashflow_remark 包含 'SHARES DIVIDENDS' 或 'SHARES WITHHOLDING TAX'
                        # 港股 判断 cashflow_type 是否为种子 & 是 HKD
                        condition_remark_dividend = data['cashflow_remark'].str.contains('SHARES DIVIDENDS', na=False)  # na=False 将 NaN 视为不包含
                        condition_remark_tax = data['cashflow_remark'].str.contains('SHARES WITHHOLDING TAX', na=False)

                        condition_remark_hk_tax = data['cashflow_type'].str.contains('现金种子', na=False) & condition_currency_hkd
                        condition_remark_us_tax = condition_remark_dividend | condition_remark_tax
                        condition_remark = condition_remark_hk_tax | condition_remark_us_tax

                        condition_currency = condition_currency_hkd | condition_currency_usd

                        # 组合所有条件
                        # 使用 & (与) 组合所有布尔Series
                        filtered_data = data[condition_currency & condition_amount & condition_remark]
                        if not filtered_data.empty:  # 判断筛选后的DataFrame是否为空
                            all_accounts_orders.append(filtered_data)
                            print(f"\n    成功筛选出 {len(filtered_data)} 条符合条件的流水记录")
                            print("筛选出的数据:")
                            print(filtered_data)
                        else:
                            print("\n    未找到符合条件的流水记录。")
                    
                    # 更新下一批次的开始时间
                    current_start = current_end

        if not all_accounts_orders:
            print("所有账户和市场都未找到任何订单记录")
            return

        # 合并所有账户和市场的数据到一个DataFrame
        final_df = pd.concat(all_accounts_orders, ignore_index=True)

        # 按时间排序
        if 'clearing_date' in final_df.columns:
            final_df = final_df.sort_values(by='clearing_date', ascending=False, kind='stable')

        final_df = final_df.drop_duplicates(subset=['cashflow_id'], keep='first')
        # 打印最终结果的汇总信息
        # print(final_df)
        out_path = os.path.join('data', 'futu_cash_flow.csv')
        # 生成目标DataFrame
        out_df = pd.DataFrame()
        out_df['id'] = final_df['cashflow_id']
        out_df['金额'] = final_df['cashflow_amount']
        out_df['结算币种'] = final_df['currency']
        out_df['交易类型'] = final_df['cashflow_type']
        out_df['交易时间'] = final_df['clearing_date']
        out_df['交易备注'] = final_df['cashflow_remark']

        # 保存为目标格式
        out_df.to_csv(out_path, index=False, encoding='utf-8-sig')

    finally:
        # 关闭连接
        quote_ctx.close()
        trade_ctx.close()

if __name__ == '__main__':
    load_dotenv()
    get_history_cash_flow()