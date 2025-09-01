# futu/cash_flow/processor.py
# This module will be responsible for processing cash flow data.

import logging
import pandas as pd

logger = logging.getLogger(__name__)

def process_cash_flow_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    处理资金流水数据，包括类型转换、重命名和排序。

    Args:
        df (pd.DataFrame): 原始资金流水DataFrame。

    Returns:
        pd.DataFrame: 处理后的DataFrame。
    """
    if df.empty:
        logger.warning("输入的DataFrame为空，无需处理。")
        return df

    processed_df = df.copy()

    # 1. 数据类型转换
    logger.info("正在进行数据类型转换...")
    processed_df['deal_time'] = pd.to_datetime(processed_df['deal_time'])
    processed_df['amount'] = pd.to_numeric(processed_df['amount'], errors='coerce')

    # 2. 列重命名
    logger.info("正在重命名列...")
    rename_map = {
        'deal_time': '交易时间',
        'currency': '币种',
        'amount': '金额',
        'description': '描述',
        'acc_id': '账户ID'
    }
    processed_df.rename(columns=rename_map, inplace=True)
    
    # 筛选出我们感兴趣的列
    final_columns = ['交易时间', '币种', '金额', '描述', '账户ID']
    processed_df = processed_df[final_columns]

    # 3. 排序
    logger.info("正在按交易时间排序...")
    processed_df.sort_values(by='交易时间', ascending=True, inplace=True)
    
    logger.info("资金流水数据处理完成。")
    return processed_df