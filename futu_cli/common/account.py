"""
富途账户管理模块
提供账户获取和过滤功能
"""
import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from futu import OpenSecTradeContext, RET_OK, TrdEnv, TrdAccType

logger = logging.getLogger(__name__)


class AccountError(Exception):
    """账户操作异常"""
    pass


def get_valid_accounts(
    trade_ctx: OpenSecTradeContext,
    exclude_simulate: bool = True,
    exclude_cash: bool = True
) -> List[Dict[str, Any]]:
    """
    获取有效的交易账户列表
    
    Args:
        trade_ctx: 交易上下文
        exclude_simulate: 是否排除模拟账户，默认True
        exclude_cash: 是否排除现金账户，默认True
        
    Returns:
        有效账户列表，每个账户包含acc_id, card_num, uni_card_num等信息
        
    Raises:
        AccountError: 获取账户失败
    """
    # 获取账户列表
    ret, acc_list_df = trade_ctx.get_acc_list()
    
    if ret != RET_OK:
        raise AccountError(f"获取账户列表失败: {acc_list_df}")
    
    if not isinstance(acc_list_df, pd.DataFrame):
        raise AccountError("账户列表数据格式错误")
    
    # 过滤有效账户
    valid_accounts = []
    
    for _, acc_row in acc_list_df.iterrows():
        if _is_valid_account(acc_row, exclude_simulate, exclude_cash):
            account_info = _extract_account_info(acc_row)
            if account_info:
                valid_accounts.append(account_info)
    
    return valid_accounts


def _is_valid_account(
    acc_row: pd.Series,
    exclude_simulate: bool,
    exclude_cash: bool
) -> bool:
    """
    判断账户是否有效
    
    Args:
        acc_row: 账户行数据
        exclude_simulate: 是否排除模拟账户
        exclude_cash: 是否排除现金账户
        
    Returns:
        是否为有效账户
    """
    # 检查账户ID是否存在
    acc_id = acc_row.get('acc_id')
    if acc_id is None:
        return False
    
    # 排除模拟账户
    if exclude_simulate and acc_row.get("trd_env") == TrdEnv.SIMULATE:
        return False
    
    # 排除现金账户
    if exclude_cash and acc_row.get("acc_type") == TrdAccType.CASH:
        return False
    
    return True


def _extract_account_info(acc_row: pd.Series) -> Optional[Dict[str, Any]]:
    """
    提取账户信息
    
    Args:
        acc_row: 账户行数据
        
    Returns:
        账户信息字典，如果提取失败返回None
    """
    acc_id = acc_row.get('acc_id')
    
    # 验证并转换账户ID
    try:
        acc_id_int = int(acc_id)
    except (ValueError, TypeError):
        logger.warning(f"无效的账户ID: {acc_id}")
        return None
    
    return {
        'acc_id': acc_id_int,
        'acc_id_str': str(acc_id),
        'card_num': acc_row.get('card_num'),
        'uni_card_num': acc_row.get('uni_card_num'),
        'trd_env': acc_row.get('trd_env'),
        'acc_type': acc_row.get('acc_type')
    }


def print_account_info(accounts: List[Dict[str, Any]]) -> None:
    """
    打印账户信息
    
    Args:
        accounts: 账户列表
    """
    logger.info(f"找到 {len(accounts)} 个有效账户:")
    for account in accounts:
        logger.info(f"  账户ID: {account['acc_id']}")
        logger.info(f"  卡号: {account['card_num']}")
        logger.info(f"  统一卡号: {account['uni_card_num']}")
        logger.info("  ---")


def get_account_by_id(accounts: List[Dict[str, Any]], acc_id: int) -> Optional[Dict[str, Any]]:
    """
    根据账户ID获取账户信息
    
    Args:
        accounts: 账户列表
        acc_id: 账户ID
        
    Returns:
        账户信息，如果未找到返回None
    """
    for account in accounts:
        if account['acc_id'] == acc_id:
            return account
    return None