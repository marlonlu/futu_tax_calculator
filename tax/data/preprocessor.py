import re
import pandas as pd
import os
from datetime import datetime
from futu_cli.common.config import load_config

# 支持香港 美国期权
OPTION_PATTERN = re.compile(r'^(US|HK)\.([A-Z0-9]+)(\d{6})([CP])(\d+)$')


def _load_direction_mapping():
    """加载买卖方向映射配置"""
    config = load_config('trading_config')
    direction_config = config.get('trading', {}).get('direction_mapping', {})
    
    replacement_map = {}
    for direction, values in direction_config.items():
        for value in values:
            replacement_map[value.lower()] = direction
    
    return replacement_map


def preprocess_data(file_path):
    """
    加载并预处理交易数据
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"输入文件未找到: {file_path}")

    df = pd.read_csv(file_path)

    # 数据清洗和类型转换
    df['交易时间'] = pd.to_datetime(df['交易时间'])
    df['数量'] = pd.to_numeric(df['数量'], errors='coerce')
    df['成交价格'] = pd.to_numeric(df['成交价格'], errors='coerce')
    df['合计手续费'] = pd.to_numeric(df['合计手续费'], errors='coerce')

    df['买卖方向'] = df['买卖方向'].str.lower().str.strip()
    # 使用配置化的买卖方向映射
    replacement_map = _load_direction_mapping()
    df['买卖方向'] = df['买卖方向'].replace(replacement_map)

    # 移除关键数据列中的NaN值
    df.dropna(subset=['数量', '成交价格', '合计手续费', '交易时间'], inplace=True)

    # 按时间排序是保证后续计算正确性的关键
    df.sort_values(by='交易时间', inplace=True)

    return df


def classify_asset(code):
    """
    根据股票代码安全地区分资产类型。
    """
    if not isinstance(code, str):
        return 'Stock'
    if OPTION_PATTERN.match(code):
        return 'Option'
    return 'Stock'


def is_buy(row):
    """判断是否为买入操作"""
    return str(row['买卖方向']).lower() == 'buy'


def is_sell(row):
    """判断是否为卖出操作"""
    return str(row['买卖方向']).lower() == 'sell'