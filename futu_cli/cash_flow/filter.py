# futu/cash_flow/filter.py
# This module will be responsible for filtering cash flow data.

import logging
from typing import List, Dict, Any
import pandas as pd
from futu import TrdEnv

logger = logging.getLogger(__name__)

def filter_accounts(accounts: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    根据配置过滤账户列表。

    Args:
        accounts (List[Dict[str, Any]]): 原始账户列表。
        config (Dict[str, Any]): 账户过滤配置，例如:
            {
                'exclude_simulate': True,
                'exclude_cash': True
            }

    Returns:
        List[Dict[str, Any]]: 过滤后的账户列表。
    """
    filtered = accounts
    
    if config.get('exclude_simulate', True):
        original_count = len(filtered)
        filtered = [acc for acc in filtered if acc.get('trd_env') != TrdEnv.SIMULATE]
        logger.info(f"根据 'exclude_simulate' 规则，从 {original_count} 个账户中过滤后剩余 {len(filtered)} 个。")

    # 注意：'acc_category' 是一个假设字段，用于标识现金账户。
    # 富途API的get_acc_list文档未明确此字段，可能需要根据实际返回数据调整。
    if config.get('exclude_cash', True):
        original_count = len(filtered)
        # 假设账户字典中有一个 'acc_category' 键，其值为 'CASH' 时表示现金账户。
        filtered = [acc for acc in filtered if acc.get('acc_category') != 'CASH']
        logger.info(f"根据 'exclude_cash' 规则，从 {original_count} 个账户中过滤后剩余 {len(filtered)} 个。")

    return filtered

def filter_cash_flow_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    根据配置过滤资金流水数据。

    Args:
        df (pd.DataFrame): 包含资金流水数据的DataFrame。
        config (Dict[str, Any]): 数据过滤配置。

    Returns:
        pd.DataFrame: 过滤后的DataFrame。
    """
    # 当前没有为资金流水数据定义额外的过滤器。
    # 这是一个占位函数，用于未来的过滤逻辑。
    logger.info("当前没有为资金流水数据定义额外的过滤器。")
    return df