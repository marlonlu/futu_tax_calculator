# futu/history/processor.py
# This module will be responsible for processing historical order data.

import logging
import os
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)

def save_raw_data(df: pd.DataFrame, config: Dict[str, Any]):
    """保存原始成交记录数据。"""
    output_config = config.get('output', {})
    raw_filename = output_config.get('raw_file', 'futu_history_raw.csv')
    
    # 确保 data 目录存在
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    
    out_path = os.path.join(data_dir, raw_filename)
    
    try:
        df.to_csv(out_path, index=False, encoding='utf-8-sig')
        logger.info(f"原始成交数据已成功保存到: {out_path}")
    except Exception as e:
        logger.error(f'保存原始数据文件失败: {e}')
        raise

def process_and_save_final_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    处理数据并保存为最终格式，用于税务计算。

    Args:
        df (pd.DataFrame): 包含费用信息的成交记录DataFrame。
        config (Dict[str, Any]): 包含历史记录配置的字典。

    Returns:
        pd.DataFrame: 处理后的精简版DataFrame。
    """
    logger.info("开始处理数据以生成最终格式...")
    
    out_df = pd.DataFrame()
    out_df['股票代码'] = df['code']
    out_df['数量'] = df['qty']
    out_df['成交价格'] = df['price']
    out_df['买卖方向'] = df['trd_side'].replace({'BUY': 'buy', 'SELL': 'sell'})
    out_df['结算币种'] = df['deal_market'].replace({'HK': 'HKD', 'US': 'USD'})
    out_df['合计手续费'] = df.get('合计手续费', 0)  # 使用.get确保列不存在时不会出错
    out_df['交易时间'] = df['create_time'].str[:19]  # 去除毫秒

    output_config = config.get('output', {})
    processed_filename = output_config.get('processed_file', 'futu_history.csv')
    
    data_dir = 'data'
    os.makedirs(data_dir, exist_ok=True)
    out_path = os.path.join(data_dir, processed_filename)

    try:
        out_df.to_csv(out_path, index=False, encoding='utf-8-sig')
        logger.info(f'处理后的历史交易数据已成功导出到: {out_path}')
    except Exception as e:
        logger.error(f'导出最终处理文件失败: {e}')
        raise
        
    return out_df

def process_history_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    对历史成交记录进行排序、保存和最终处理。

    Args:
        df (pd.DataFrame): 包含费用信息的成交记录DataFrame。
        config (Dict[str, Any]): 包含历史记录配置的字典。

    Returns:
        pd.DataFrame: 最终处理过的精简版DataFrame。
    """
    if df.empty:
        logger.warning("输入的DataFrame为空，无需处理。")
        return df

    # 按时间升序排序
    if 'create_time' in df.columns:
        logger.info("正在按创建时间对数据进行排序...")
        df.sort_values(by='create_time', ascending=True, inplace=True, ignore_index=True)
    else:
        logger.warning("缺少 'create_time' 列，无法进行时间排序。")

    # 1. 保存一份原始数据
    save_raw_data(df, config)
    
    # 2. 处理并保存为最终格式
    final_df = process_and_save_final_data(df, config)
    
    return final_df