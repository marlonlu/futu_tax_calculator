# futu/cash_flow/fetcher.py
# This module will be responsible for fetching cash flow data.

import logging
import pandas as pd
from futu import RET_OK, TrdEnv

from futu_cli.common.connection import create_futu_connections, close_connections
from futu_cli.cash_flow.filter import filter_accounts

logger = logging.getLogger(__name__)


def fetch_cash_flow(conn_config: dict, cash_flow_config: dict) -> pd.DataFrame:
    """
    获取所有适用账户的资金流水记录。

    该函数会：
    1. 使用传入的连接和现金流相关配置。
    2. 创建到富途的连接。
    3. 获取账户列表并根据配置进行过滤。
    4. 遍历账户，获取指定时间范围内的资金流水。
    5. 将所有流水合并为一个DataFrame。
    6. 安全关闭连接并返回数据。

    Args:
        conn_config (dict): 连接配置字典
        cash_flow_config (dict): 现金流配置字典

    Returns:
        pd.DataFrame: 包含所有账户资金流水的DataFrame，如果无数据则返回空DataFrame。
    
    Raises:
        Exception: 如果在获取数据过程中发生错误。
    """
    quote_ctx = None
    trade_ctx = None
    all_cash_flow = []

    try:
        # 1. 从传入的配置中提取参数
        connection_settings = conn_config.get('connection', {})
        time_range = cash_flow_config.get('time_range', {})
        start_date = time_range.get('start_date')
        end_date = time_range.get('end_date')
        
        account_config = cash_flow_config.get('account', {})

        # 2. 创建连接
        logger.info("正在创建富途连接...")
        quote_ctx, trade_ctx = create_futu_connections(
            host=connection_settings.get('default_host'),
            port=connection_settings.get('default_port')
        )

        # 3. 获取并过滤账户
        logger.info("正在获取账户列表...")
        ret, acc_list = trade_ctx.get_acc_list()
        if ret != RET_OK:
            raise Exception(f"获取账户列表失败: {acc_list}")

        filtered_accounts = filter_accounts(acc_list, account_config)
        
        if not filtered_accounts:
            logger.warning("没有找到符合条件的交易账户。")
            return pd.DataFrame()

        # 4. 遍历账户获取资金流水
        for acc in filtered_accounts:
            acc_id = acc['acc_id']
            logger.info(f"正在为账户 {acc_id} 获取资金流水...")
            
            ret, data = trade_ctx.get_funds_statement(
                acc_id=acc_id,
                start_date=start_date,
                end_date=end_date
            )
            
            if ret == RET_OK:
                if not data.empty:
                    data['acc_id'] = acc_id  # 添加账户ID列以便区分
                    all_cash_flow.append(data)
                else:
                    logger.info(f"账户 {acc_id} 在指定时间内无资金流水。")
            else:
                logger.error(f"为账户 {acc_id} 获取资金流水失败: {data}")

        # 5. 合并数据
        if not all_cash_flow:
            logger.warning("所有账户在指定时间内均无资金流水。")
            return pd.DataFrame()
            
        final_df = pd.concat(all_cash_flow, ignore_index=True)
        logger.info(f"成功获取 {len(final_df)} 条资金流水记录。")
        
        return final_df

    except Exception as e:
        logger.error(f"获取资金流水过程中发生错误: {e}")
        raise
    finally:
        # 6. 关闭连接
        if quote_ctx and trade_ctx:
            logger.info("正在关闭富途连接...")
            close_connections(quote_ctx, trade_ctx)