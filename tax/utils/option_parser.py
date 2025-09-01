import re
import logging
from datetime import datetime, date

# 配置日志
logger = logging.getLogger(__name__)

# 支持香港 美国期权的正则表达式
OPTION_PATTERN = re.compile(r'^(US|HK)\.([A-Z0-9]+)(\d{6})([CP])(\d+)$')


def classify_asset(code):
    """
    根据股票代码安全地区分资产类型。
    """
    if not isinstance(code, str):
        return 'Stock'
    if OPTION_PATTERN.match(code):
        return 'Option'
    return 'Stock'


def is_expiration_future_or_current(expiration_date_str) -> bool:
    """
    判断期权是否已到期
    """
    if not expiration_date_str:
        # 如果输入是 None 或空字符串，直接认为是过期的
        logger.warning("Invalid date expiration_date_str.")
        return False
    try:
        # 2. 将输入的字符串转换为 date 对象
        #    我们只关心日期，不需要时间部分，所以使用 date 对象进行比较更精确。
        expiration_date = datetime.strptime(expiration_date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        # 如果字符串格式不正确 (e.g., '2024-4-19') 或类型错误 (e.g., 传入一个整数)
        logger.warning(f"Invalid date format or type for input '{expiration_date_str}'. Treated as expired.")
        return False

    # 3. 获取今天的日期
    current_date = date.today()
    # 4. 核心逻辑：比较日期
    #    如果到期日大于或等于今天，则它没有过期。
    return expiration_date >= current_date


def extract_expiration_date(option_code):
    """
    从多种格式的期权代码中解析到期日，并将其格式化为 'YYYY-MM-DD'。
    兼容格式:
    - TSLA240419C200000 (标准美股)
    - US.KWEB250919C36000 (带 'US.' 前缀)
    - HK.TCH251030C550000 (带 'HK.' 前缀)
    Args:
        option_code (str): 期权代码字符串。
    Returns:
        str | None: 格式化后的日期 'YYYY-MM-DD'，如果格式不匹配则返回 None。
    """
    # 步骤 1: 构建一个更通用的正则表达式
    pattern = re.compile(r'^(?:(?:US|HK)\.)?[A-Z0-9]+(\d{6})[CP]\d+$')

    match = pattern.match(option_code)
    if not match:
        logger.warning(f"Code '{option_code}' does not match any known option format.")
        return None
    # 步骤 2: 提取日期字符串
    # 无论是否有前缀，日期始终是唯一的那个捕获组 (group 1)
    date_str = match.group(1)  # -> '240419', '250919', '251030'
    # 步骤 3: 转换格式 (此部分逻辑不变)
    try:
        expiration_date_obj = datetime.strptime(date_str, '%y%m%d')
        return expiration_date_obj.strftime('%Y-%m-%d')
    except ValueError:
        logger.warning(f"Extracted date string '{date_str}' from '{option_code}' is not valid.")
        return None