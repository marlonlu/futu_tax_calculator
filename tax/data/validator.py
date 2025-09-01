import pandas as pd
import os
import logging

# 配置日志
logger = logging.getLogger(__name__)


def validate_file_exists(file_path):
    """
    验证文件是否存在
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"输入文件未找到: {file_path}")
    return True


def validate_dataframe_columns(df, required_columns):
    """
    验证DataFrame是否包含必需的列
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"缺少必需的列: {missing_columns}")
    return True


def validate_transaction_data(df):
    """
    验证交易数据的完整性
    """
    required_columns = ['交易时间', '数量', '成交价格', '合计手续费', '买卖方向', '股票代码', '结算币种']
    
    # 检查必需列
    validate_dataframe_columns(df, required_columns)
    
    # 检查数据类型和范围
    if df.empty:
        raise ValueError("数据文件为空")
    
    # 检查数值列是否有效
    numeric_columns = ['数量', '成交价格', '合计手续费']
    for col in numeric_columns:
        if df[col].isna().all():
            raise ValueError(f"列 '{col}' 全部为空值")
        if (df[col] < 0).any():
            logger.warning(f"列 '{col}' 包含负值")
    
    return True


def clean_and_validate_data(df):
    """
    清洗并验证数据
    """
    # 验证基本结构
    validate_transaction_data(df)
    
    # 移除关键数据列中的NaN值
    initial_count = len(df)
    df_clean = df.dropna(subset=['数量', '成交价格', '合计手续费', '交易时间']).copy()
    final_count = len(df_clean)
    
    if final_count < initial_count:
        logger.warning(f"移除了 {initial_count - final_count} 行包含空值的数据")
    
    if df_clean.empty:
        raise ValueError("清洗后数据为空，请检查数据质量")
    
    return df_clean