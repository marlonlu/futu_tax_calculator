# futu/history/fetcher.py
# This module will be responsible for fetching historical order and deal data.

import logging
from datetime import datetime, timedelta
import pandas as pd
from futu import OpenSecTradeContext, TrdMarket, RET_OK
from typing import List, Dict, Any

from futu_cli.common.account import get_valid_accounts
from futu_cli.common.rate_limiter import create_rate_limiter

logger = logging.getLogger(__name__)


def _fetch_deals_for_account(
    trade_ctx: OpenSecTradeContext,
    acc_id: int,
    config: Dict[str, Any],
    rate_limiter
) -> List[pd.DataFrame]:
    """为单个账户获取历史成交记录。"""
    deals_for_account = []
    
    time_config = config.get('time_range', {})
    start_date = datetime.strptime(time_config.get('start_date', '2021-01-01'), '%Y-%m-%d')
    end_date = datetime.strptime(time_config.get('end_date', '2025-01-01'), '%Y-%m-%d')
    batch_days = config.get('batch_processing', {}).get('time_batch_days', 90)

    logger.debug(f"  ...正在为账户 {acc_id} 查询市场: TrdMarket.NONE")
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=batch_days), end_date)
        logger.info(f"正在为账户 {acc_id} 获取 {current_start.strftime('%Y-%m-%d')} 到 {current_end.strftime('%Y-%m-%d')} 的成交数据...")

        rate_limiter.wait_if_needed()

        ret, data = trade_ctx.history_deal_list_query(
            acc_id=acc_id,
            deal_market=TrdMarket.NONE, # 直接使用 TrdMarket.NONE
            start=current_start.strftime('%Y-%m-%d %H:%M:%S'),
            end=current_end.strftime('%Y-%m-%d %H:%M:%S'),
        )

        if ret != RET_OK:
            error_msg = f'获取账户 {acc_id} 的历史成交失败: {data}'
            logger.critical(error_msg)
            raise Exception(error_msg)

        if isinstance(data, pd.DataFrame) and not data.empty:
            data['acc_id'] = acc_id
            deals_for_account.append(data)
            logger.info(f"    成功获取 {len(data)} 条成交记录。")

        current_start = current_end
            
    return deals_for_account


def _perform_deal_fetch(trade_ctx: OpenSecTradeContext, config: dict) -> pd.DataFrame:
    """内部函数，执行实际的历史成交数据获取逻辑。"""
    rate_limiter = create_rate_limiter(max_requests=9, time_window=30)
    all_deals = []

    accounts = get_valid_accounts(trade_ctx)
    if not accounts:
        logger.warning("未找到有效账户。")
        return pd.DataFrame()

    for account in accounts:
        acc_id = account['acc_id']
        deals = _fetch_deals_for_account(trade_ctx, acc_id, config, rate_limiter)
        all_deals.extend(deals)

    if not all_deals:
        logger.warning("所有账户和市场均未找到任何成交记录。")
        return pd.DataFrame()

    final_df = pd.concat(all_deals, ignore_index=True)
    logger.info(f"总共获取 {len(final_df)} 条成交记录。")
    return final_df


def fetch_history_deals(trade_ctx: OpenSecTradeContext, config: dict) -> pd.DataFrame:
    """
    获取所有有效账户的历史成交记录。
    这是一个用于异常处理的包装函数。

    Args:
        trade_ctx (OpenSecTradeContext): 富途交易上下文。
        config (dict): 包含历史记录配置的字典。

    Returns:
        pd.DataFrame: 包含所有账户历史成交记录的DataFrame。
    """
    try:
        return _perform_deal_fetch(trade_ctx, config)
    except Exception as e:
        logger.error(f"获取历史成交记录过程中发生错误: {e}", exc_info=True)
        raise