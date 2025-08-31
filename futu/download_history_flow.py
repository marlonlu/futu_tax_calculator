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

def get_history_orders():
    host = os.environ.get("FUTU_ADDRESS", "").strip()
    port = int(os.environ.get("FUTU_PORT"))
    is_local_futu_api = host == "127.0.0.1"
    if is_local_futu_api:
        # 创建OpenD连接
        quote_ctx = OpenQuoteContext(host=host, port=port)
        # 不指定市场，获取所有市场的交易权限
        trade_ctx = OpenSecTradeContext(host=host, port=port, filter_trdmarket=TrdMarket.NONE, is_encrypt=False)
    else:
        # 不是本地网络请求必须要设置 rsa
        SysConfig.INIT_RSA_FILE = os.environ.get("FUTU_RSA")
        # 创建OpenD连接
        quote_ctx = OpenQuoteContext(host=host, port=port)
        # 不指定市场，获取所有市场的交易权限
        trade_ctx = OpenSecTradeContext(host=host, port=port, filter_trdmarket=TrdMarket.NONE, is_encrypt=True)

    # 创建请求限制器（30秒内最多10次请求）
    rate_limiter = RateLimiter(max_requests=9, time_window=30)
    
    # 存储所有账户的所有订单
    all_accounts_orders = []
    
    # 定义要查询的市场列表
    # markets_to_query = [TrdMarket.US, TrdMarket.HK]
    markets_to_query = [TrdMarket.NONE]
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
            if acc_row.get("trd_env")==TrdEnv.SIMULATE or acc_row.get("acc_type")==TrdAccType.CASH:
                continue
            if acc_id is None:
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
                
                # 设置查询时间范围， 目前追溯 2022，提前 1 年获取 避免某些卖出单没有找到买入单
                start_date = datetime(2021, 1, 1)
                # 如果是美股，那么这里就是美东区 冬令时，也就意味着 订单记录上 > 12/31 11:00 时间代表已经上 2025年 1.1日
                end_date = datetime(2025, 1, 1)

                # 每3个月为一个批次
                current_start = start_date
                
                while current_start < end_date:
                    # 计算当前批次的结束时间
                    current_end = min(current_start + timedelta(days=90), end_date)
                    
                    print(f"正在获取 {current_start.strftime('%Y-%m-%d %H:%M:%S')} 到 {current_end.strftime('%Y-%m-%d %H:%M:%S')} 的订单数据...")

                    # 等待请求限制
                    rate_limiter.wait_if_needed()

                    # 查询历史订单, 明确指定市场
                    ret, data = trade_ctx.history_deal_list_query(
                        acc_id=acc_id,
                        deal_market=market,
                        start=current_start.strftime('%Y-%m-%d %H:%M:%S'),
                        end=current_end.strftime('%Y-%m-%d %H:%M:%S'),
                    )
                    
                    if ret != RET_OK:
                        print(f'    ❌ ❌ 获取历史订单失败: {data} ❌ ❌ ')
                        return
                    
                    if isinstance(data, pd.DataFrame) and not data.empty:
                        data['acc_id'] = acc_id  # 新增：为每个订单加上acc_id
                        all_accounts_orders.append(data)
                        print(f"    成功获取 {len(data)} 条订单记录")
                    elif data is not None:
                        # 如果不是DataFrame但有内容，尝试转为DataFrame并追加acc_id
                        try:
                            data_df = pd.DataFrame(data)
                            if not data_df.empty:
                                data_df['acc_id'] = acc_id
                                all_accounts_orders.append(data_df)
                                print(f"    成功获取 {len(data_df)} 条订单记录 (非DataFrame原始类型)")
                        except Exception as e:
                            print(f"    数据无法转为DataFrame: {e}")
                    
                    # 更新下一批次的开始时间
                    current_start = current_end

        if not all_accounts_orders:
            print("所有账户和市场都未找到任何订单记录")
            return

        # 合并所有账户和市场的数据到一个DataFrame
        final_df = pd.concat(all_accounts_orders, ignore_index=True)

        # 按时间排序
        if 'create_time' in final_df.columns:
            final_df = final_df.sort_values(by='create_time', ascending=False, kind='stable')

        # ====== 新增：批量获取订单费用 ======
        if 'order_id' in final_df.columns and 'acc_id' in final_df.columns:
            fee_list = []
            batch_size = 400
            # 按账户分组批量查费用
            for acc_id_val, group in final_df.groupby('acc_id'):
                # 只处理int或str类型的acc_id
                if not isinstance(acc_id_val, (int, str)):
                    print(f'不支持的acc_id类型: {type(acc_id_val)}, 跳过该分组')
                    continue
                try:
                    acc_id_int = int(str(acc_id_val))
                except Exception:
                    print(f'无法转换acc_id: {acc_id_val}，跳过该分组')
                    continue
                order_ids = group['order_id'].tolist()
                for i in range(0, len(order_ids), batch_size):
                    batch_ids = order_ids[i:i+batch_size]
                    ret, fee_df = trade_ctx.order_fee_query(order_id_list=batch_ids, acc_id=acc_id_int, trd_env=TrdEnv.REAL)
                    if ret == RET_OK and isinstance(fee_df, pd.DataFrame):
                        fee_list.append(fee_df[['order_id', 'fee_amount']])
                    else:
                        print(f'acc_id={acc_id_int} 获取订单费用失败:', fee_df)
            if fee_list:
                all_fee_df = pd.concat(fee_list, ignore_index=True)
            else:
                all_fee_df = pd.DataFrame(columns=['order_id', 'fee_amount'])
            # 合并费用到订单表
            final_df = final_df.merge(all_fee_df, on='order_id', how='left')
            final_df.rename(columns={'fee_amount': '合计手续费'}, inplace=True)
        else:
            final_df['合计手续费'] = 0

        # 组合策略里 订单 id 都是同一个，导致合计手续费被重复计算了，只统计第一个，其他改为 0
        # 1. 同样，先排序
        final_df.sort_values(by=['order_id', 'create_time'], inplace=True, ignore_index=True)
        # 2. 对每个 order_id 组生成一个累积计数
        # 只有计数器 > 0 的行需要被修改
        mask = final_df.groupby('order_id').cumcount() > 0
        # 3. 使用 mask 更新费用
        final_df.loc[mask, '合计手续费'] = 0

        # 4 重新按时间排序
        # ignore_index=True 表示排序后重新生成一个从 0 开始的连续索引，非常推荐！
        final_df = final_df.sort_values(by='create_time', ascending=True, ignore_index=True)
        # ====== 新增结束 ======
        
        # 打印最终结果的汇总信息
        # print(final_df)
        
        # 保存结果到统一的CSV文件
        # 路径
        out_path = os.path.join('..', 'data', 'futu_history_raw.csv')
        os.makedirs('data', exist_ok=True)
        final_df.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"\n所有账户数据已合并保存到 {out_path}")

        # final 再精简下表结构
        # 路径
        out_path = os.path.join('..', 'data', 'futu_history.csv')
        # 生成目标DataFrame
        out_df = pd.DataFrame()
        out_df['股票代码'] = final_df['code']
        out_df['数量'] = final_df['qty']
        out_df['成交价格'] = final_df['price']
        out_df['买卖方向'] = final_df['trd_side'].replace({'BUY': 'buy', 'SELL': 'sell'})
        out_df['结算币种'] = final_df['deal_market'].replace({'HK': 'HKD', 'US': 'USD'})
        out_df['合计手续费'] = final_df["合计手续费"]  # futu原始数据无手续费字段
        out_df['交易时间'] = final_df['create_time'].str[:19]  # 去除毫秒

        # 保存为目标格式
        out_df.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f'已导出到 {out_path}')
                
    finally:
        # 关闭连接
        quote_ctx.close()
        trade_ctx.close()

if __name__ == '__main__':
    load_dotenv()
    get_history_orders() 