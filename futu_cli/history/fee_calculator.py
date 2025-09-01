# futu/history/fee_calculator.py
# This module will be responsible for calculating transaction fees.

import logging
import pandas as pd
from futu import OpenSecTradeContext, TrdEnv, RET_OK
from typing import Dict, Any

logger = logging.getLogger(__name__)

def _fetch_fees_for_account(trade_ctx: OpenSecTradeContext, acc_id: int, order_ids: list, batch_size: int) -> pd.DataFrame:
    """为单个账户批量获取订单费用。"""
    fee_list = []
    for i in range(0, len(order_ids), batch_size):
        batch_ids = order_ids[i:i+batch_size]
        ret, fee_df = trade_ctx.order_fee_query(order_id_list=batch_ids, acc_id=acc_id, trd_env=TrdEnv.REAL)
        if ret == RET_OK and isinstance(fee_df, pd.DataFrame):
            fee_list.append(fee_df[['order_id', 'fee_amount']])
        else:
            logger.error(f'为账户 {acc_id} 获取订单费用失败: {fee_df}')
    
    if not fee_list:
        return pd.DataFrame(columns=['order_id', 'fee_amount'])
        
    return pd.concat(fee_list, ignore_index=True)

def _deduplicate_combo_fees(df: pd.DataFrame) -> pd.DataFrame:
    """处理组合订单中重复计算的费用。"""
    if 'order_id' not in df.columns or 'create_time' not in df.columns:
        logger.warning("缺少 'order_id' 或 'create_time' 列，无法处理组合订单费用。")
        return df

    logger.info("正在处理组合订单的重复费用...")
    # 确保排序稳定，以便 cumcount 结果可预测
    df.sort_values(by=['order_id', 'create_time'], inplace=True, ignore_index=True)
    
    # 对每个 order_id 组，除了第一个条目外，其余的费用都设为0
    mask = df.groupby('order_id').cumcount() > 0
    df.loc[mask, '合计手续费'] = 0
    
    logger.info("组合订单费用处理完成。")
    return df

def fetch_and_calculate_fees(trade_ctx: OpenSecTradeContext, deals_df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    获取并计算历史订单的费用，并将其合并到成交记录中。

    Args:
        trade_ctx (OpenSecTradeContext): 富途交易上下文。
        deals_df (pd.DataFrame): 包含历史成交记录的DataFrame。
        config (Dict[str, Any]): 包含历史记录配置的字典。

    Returns:
        pd.DataFrame: 添加了“合计手续费”列的DataFrame。
    """
    if 'order_id' not in deals_df.columns or 'acc_id' not in deals_df.columns:
        logger.warning("成交记录DataFrame中缺少 'order_id' 或 'acc_id'，无法获取费用。")
        deals_df['合计手续费'] = 0
        return deals_df

    logger.info("开始批量获取订单费用...")
    
    all_fees_list = []
    batch_size = config.get('batch_processing', {}).get('fee_query_batch_size', 400)

    # 按账户分组批量查询费用
    for acc_id, group in deals_df.groupby('acc_id'):
        try:
            acc_id_int = int(acc_id)
        except (ValueError, TypeError):
            logger.error(f'无法将 acc_id: {acc_id} 转换为整数，跳过该账户的费用查询。')
            continue

        order_ids = group['order_id'].unique().tolist()
        logger.info(f"正在为账户 {acc_id_int} 获取 {len(order_ids)} 个唯一订单的费用...")
        
        account_fees_df = _fetch_fees_for_account(trade_ctx, acc_id_int, order_ids, batch_size)
        if not account_fees_df.empty:
            all_fees_list.append(account_fees_df)

    if not all_fees_list:
        logger.warning("未能获取到任何订单的费用信息。")
        deals_df['合计手续费'] = 0
        return deals_df

    # 合并所有账户的费用信息
    all_fees_df = pd.concat(all_fees_list, ignore_index=True)
    logger.info(f"成功获取 {len(all_fees_df)} 条费用记录。")

    # 合并费用到主DataFrame
    deals_df = deals_df.merge(all_fees_df, on='order_id', how='left')
    deals_df.rename(columns={'fee_amount': '合计手续费'}, inplace=True)
    deals_df['合计手续费'].fillna(0, inplace=True) # 对于没有费用的订单，填充为0

    # 处理组合订单的重复费用
    deals_df = _deduplicate_combo_fees(deals_df)

    return deals_df