"""
股票和期权税务计算器

处理富途证券的交易历史数据，计算股票和期权的税务报告。
支持移动平均加权算法、期权到期处理和多币种汇总。
"""
import argparse
import logging
import os
import re
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple, Any

import pandas as pd

# ==============================================================================
# 模块级常量与配置
# ==============================================================================

# 支持香港和美国期权的正则表达式模式
OPTION_PATTERN = re.compile(r'^(US|HK)\.([A-Z0-9]+)(\d{6})([CP])(\d+)$')

# 买卖方向标准化映射
DIRECTION_MAPPING = {
    'orderside.sell': 'sell',
    'orderside.buy': 'buy',
    '卖出': 'sell',
    '买入': 'buy',
    'sell_short': 'sell',
    'buy_back': 'buy'
}

# 期权价格乘数
OPTION_PRICE_MULTIPLIER = 100

def _setup_logging(level: str = 'INFO') -> logging.Logger:
    """配置日志系统。"""
    logger = logging.getLogger('tax_calculator')
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(getattr(logging, level.upper()))
    return logger

# 创建全局logger实例
logger = _setup_logging()


# ==============================================================================
# 数据加载与预处理 (一级函数)
# ==============================================================================

def preprocess_data(file_path: str) -> pd.DataFrame:
    """加载并预处理交易数据。"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"输入文件未找到: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        raise ValueError(f"读取CSV文件失败: {e}")

    _validate_dataframe_columns(df)
    cleaned_df = _clean_trading_data(df)

    if cleaned_df.empty:
        raise ValueError("处理后的数据为空，请检查输入文件的数据质量")

    return cleaned_df

def _validate_dataframe_columns(df: pd.DataFrame) -> None:
    """验证DataFrame是否包含必需的列。"""
    required_columns = ['交易时间', '数量', '成交价格', '合计手续费', '买卖方向', '股票代码', '结算币种']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"CSV文件缺少必需的列: {missing_columns}")

def _clean_trading_data(df: pd.DataFrame) -> pd.DataFrame:
    """清洗交易数据的纯函数。"""
    cleaned_df = df.copy()

    cleaned_df['交易时间'] = pd.to_datetime(cleaned_df['交易时间'], errors='coerce')
    cleaned_df['数量'] = pd.to_numeric(cleaned_df['数量'], errors='coerce')
    cleaned_df['成交价格'] = pd.to_numeric(cleaned_df['成交价格'], errors='coerce')
    cleaned_df['合计手续费'] = pd.to_numeric(cleaned_df['合计手续费'], errors='coerce')

    cleaned_df['买卖方向'] = cleaned_df['买卖方向'].str.lower().str.strip().replace(DIRECTION_MAPPING)

    before_count = len(cleaned_df)
    cleaned_df.dropna(subset=['数量', '成交价格', '合计手续费', '交易时间', '买卖方向'], inplace=True)
    after_count = len(cleaned_df)

    if before_count > after_count:
        logger.warning(f"移除了 {before_count - after_count} 行包含无效数据的记录")

    return cleaned_df.sort_values(by='交易时间', ignore_index=True)


# ==============================================================================
# 工具与辅助函数
# ==============================================================================

def classify_asset(code: str) -> str:
    """根据股票代码安全地区分资产类型。"""
    return 'Option' if isinstance(code, str) and OPTION_PATTERN.match(code) else 'Stock'

def is_buy(row: pd.Series) -> bool:
    """判断是否为买入操作 (已标准化)。"""
    return row.买卖方向 == 'buy'

def is_sell(row: pd.Series) -> bool:
    """判断是否为卖出操作 (已标准化)。"""
    return row.买卖方向 == 'sell'

def create_sales_record(
    code: str, sale_price: float, cost_price: float, quantity: int,
    profit: float, trade_time: Any, currency: str, note: str = ''
) -> Dict[str, Any]:
    """创建销售记录的纯函数。"""
    return {
        '股票代码': code, '卖出价格': sale_price, '成本价': cost_price,
        '数量': quantity, '利润': round(profit, 4), '时间': trade_time,
        '结算币种': currency, '备注': note
    }

def extract_expiration_date(option_code: str) -> Optional[str]:
    """从期权代码中解析到期日。"""
    match = OPTION_PATTERN.match(option_code)
    if not match:
        logger.warning(f"代码 '{option_code}' 不匹配期权格式，无法解析到期日。")
        return None
    try:
        return datetime.strptime(match.group(3), '%y%m%d').strftime('%Y-%m-%d')
    except ValueError:
        logger.warning(f"从'{option_code}'中解析的日期'{match.group(3)}'无效。")
        return None


# ==============================================================================
# 股票交易处理
# ==============================================================================

def process_stock_transactions(df: pd.DataFrame, code: str) -> List[Dict[str, Any]]:
    """处理单个股票的完整历史交易记录，使用移动平均加权算法。"""
    holdings = {'quantity': 0, 'cost_basis': 0.0}
    sales_records = []

    for row in df.itertuples(index=False):
        if is_buy(row):
            holdings['quantity'] += row.数量
            holdings['cost_basis'] += row.数量 * row.成交价格 + row.合计手续费
        elif is_sell(row):
            if holdings['quantity'] == 0:
                sales_records.append(create_sales_record(
                    code, row.成交价格, 0, row.数量, 0, row.交易时间,
                    row.结算币种, '卖出时无持仓, 需手动核查'
                ))
                continue

            avg_cost = holdings['cost_basis'] / holdings['quantity']
            sold_qty = min(row.数量, holdings['quantity'])

            profit = (row.成交价格 - avg_cost) * sold_qty - row.合计手续费
            sales_records.append(create_sales_record(
                code, row.成交价格, round(avg_cost, 4), sold_qty, profit,
                row.交易时间, row.结算币种
            ))

            holdings['quantity'] -= sold_qty
            holdings['cost_basis'] -= avg_cost * sold_qty

            if row.数量 > sold_qty:
                extra_qty = row.数量 - sold_qty
                sales_records.append(create_sales_record(
                    code, row.成交价格, 0, extra_qty, 0, row.交易时间,
                    row.结算币种, '卖超持仓, 需手动核查'
                ))
                holdings = {'quantity': 0, 'cost_basis': 0.0}
    return sales_records


# ==============================================================================
# 期权交易处理 (重构核心)
# ==============================================================================

def _handle_sell_to_close(holdings: Dict, row: Any) -> Tuple[Dict, Dict]:
    """处理期权卖出平仓（多头平仓）。返回 (销售记录, 更新后的持仓)。"""
    sell_quantity = min(row.数量, holdings['quantity'])
    avg_cost = holdings['cost_basis'] / holdings['quantity']

    profit = (row.成交价格 * OPTION_PRICE_MULTIPLIER * sell_quantity) - (avg_cost * sell_quantity) - row.合计手续费
    record = create_sales_record(
        row.股票代码, row.成交价格, round(avg_cost / OPTION_PRICE_MULTIPLIER, 4),
        sell_quantity, profit, row.交易时间, row.结算币种
    )

    updated_holdings = holdings.copy()
    updated_holdings['quantity'] -= sell_quantity
    updated_holdings['cost_basis'] -= avg_cost * sell_quantity
    return record, updated_holdings

def _handle_buy_to_close(holdings: Dict, row: Any) -> Tuple[Dict, Dict]:
    """处理期权买入平仓（空头平仓）。返回 (销售记录, 更新后的持仓)。"""
    close_quantity = min(row.数量, abs(holdings['quantity']))
    avg_proceeds = holdings['short_proceeds'] / abs(holdings['quantity'])

    profit = (avg_proceeds * close_quantity) - (row.成交价格 * OPTION_PRICE_MULTIPLIER * close_quantity) - row.合计手续费
    record = create_sales_record(
        row.股票代码, round(avg_proceeds / OPTION_PRICE_MULTIPLIER, 4), row.成交价格,
        close_quantity, profit, row.交易时间, row.结算币种, '卖空平仓'
    )

    updated_holdings = holdings.copy()
    updated_holdings['quantity'] += close_quantity
    updated_holdings['short_proceeds'] -= avg_proceeds * close_quantity
    return record, updated_holdings

def _handle_option_expiration(holdings: Dict, code: str, last_row: Any) -> Optional[Dict]:
    """处理到期时未平仓的期权头寸。"""
    expiration_date = extract_expiration_date(code) or last_row.交易时间
    exp_note = '到期日无法解析，使用最后交易日代替' if not extract_expiration_date(code) else ''

    if holdings['quantity'] > 0:  # 多头到期作废
        cost = holdings['cost_basis'] / holdings['quantity'] / OPTION_PRICE_MULTIPLIER
        return create_sales_record(
            code, 0, round(cost, 4), holdings['quantity'], -holdings['cost_basis'],
            expiration_date, last_row.结算币种, f'期权到期作废 {exp_note}'.strip()
        )
    elif holdings['quantity'] < 0:  # 空头到期获利
        sell_price = holdings['short_proceeds'] / abs(holdings['quantity']) / OPTION_PRICE_MULTIPLIER
        return create_sales_record(
            code, round(sell_price, 4), 0, abs(holdings['quantity']), holdings['short_proceeds'],
            expiration_date, last_row.结算币种, f'卖空期权到期 {exp_note}'.strip()
        )
    return None

def process_option_transactions(df: pd.DataFrame, code: str) -> List[Dict[str, Any]]:
    """调度器：处理单个期权的交易记录。"""
    holdings = {'quantity': 0, 'cost_basis': 0.0, 'short_proceeds': 0.0}
    sales_records = []

    for row in df.itertuples(index=False):
        qty, price, fee = row.数量, row.成交价格, row.合计手续费

        if is_buy(row):
            if holdings['quantity'] < 0:  # 买入平仓
                record, holdings = _handle_buy_to_close(holdings, row)
                sales_records.append(record)
                if qty > abs(record['数量']): # 买入量大于平仓量，剩余部分开多仓
                    open_qty = qty - abs(record['数量'])
                    holdings['quantity'] += open_qty
                    holdings['cost_basis'] += open_qty * price * OPTION_PRICE_MULTIPLIER
            else:  # 买入开仓
                holdings['quantity'] += qty
                holdings['cost_basis'] += qty * price * OPTION_PRICE_MULTIPLIER + fee

        elif is_sell(row):
            if holdings['quantity'] > 0:  # 卖出平仓
                record, holdings = _handle_sell_to_close(holdings, row)
                sales_records.append(record)
                if qty > record['数量']: # 卖出量大于平仓量，剩余部分开空仓
                    open_qty = qty - record['数量']
                    holdings['quantity'] -= open_qty
                    holdings['short_proceeds'] += open_qty * price * OPTION_PRICE_MULTIPLIER
            else:  # 卖出开仓
                holdings['quantity'] -= qty
                holdings['short_proceeds'] += qty * price * OPTION_PRICE_MULTIPLIER - fee

    # 期末处理
    if holdings['quantity'] != 0:
        expiration_record = _handle_option_expiration(holdings, code, df.iloc[-1])
        if expiration_record:
            sales_records.append(expiration_record)

    return sales_records


# ==============================================================================
# 报告生成与保存 (重构核心)
# ==============================================================================

def _process_all_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """纯计算函数：处理所有资产，返回包含所有销售记录的DataFrame。"""
    all_sales_records = []
    for code, group_df in df.groupby('股票代码'):
        asset_type = group_df['资产类型'].iloc[0]
        logger.info(f"正在处理资产: {code} ({asset_type})")

        if asset_type == 'Stock':
            results = process_stock_transactions(group_df, code)
        else:  # Option
            results = process_option_transactions(group_df, code)
        all_sales_records.extend(results)

    if not all_sales_records:
        return pd.DataFrame()

    return pd.DataFrame(all_sales_records)

def _create_summary_df(yearly_df: pd.DataFrame) -> pd.DataFrame:
    """纯汇总函数：为年度数据生成按币种的汇总DataFrame。"""
    def summarize_currency(group):
        total_profit = group['利润'].sum()
        return pd.Series({
            '股票代码': f'{group.name[0]}年度汇总 ({group.name[1]})',
            '卖出价格': 0, '成本价': 0, '数量': 0,
            '利润': total_profit, '时间': datetime(group.name[0], 12, 31),
            '备注': f'该币种总盈利/亏损'
        })

    if yearly_df.empty:
        return pd.DataFrame()

    yearly_df['年份'] = pd.to_datetime(yearly_df['时间']).dt.year
    summary_df = yearly_df.groupby(['年份', '结算币种']).apply(summarize_currency).reset_index()
    return summary_df

def generate_and_save_reports(report_df: pd.DataFrame, output_dir: str):
    """I/O与流程控制：按年份生成并保存报告，包含汇总信息。"""
    if report_df.empty:
        logger.info("没有发现任何可报告的卖出交易，未生成报告。")
        return

    report_df['时间'] = pd.to_datetime(report_df['时间'])
    report_df['年份'] = report_df['时间'].dt.year

    summary_df = _create_summary_df(report_df.copy())

    full_report_df = pd.concat([report_df, summary_df], ignore_index=True)

    for year, year_df in full_report_df.groupby('年份'):
        final_df = year_df.sort_values(by='时间').drop(columns=['年份'])
        output_filename = os.path.join(output_dir, f"{year}_report.csv")
        final_df.to_csv(output_filename, index=False, encoding='utf-8-sig', float_format='%.4f')
        logger.info("税务报告已保存至: %s", output_filename)


# ==============================================================================
# 主逻辑与执行入口
# ==============================================================================

def _merge_rsu_data(transactions_df: pd.DataFrame, input_dir: str) -> pd.DataFrame:
    """检查、加载并合并RSU数据。"""
    rsu_file_path = os.path.join(input_dir, 'futu_rsu_history.csv')
    if os.path.exists(rsu_file_path):
        logger.info("检测到 RSU 历史文件，开始合并处理...")
        try:
            rsu_df = preprocess_data(rsu_file_path)
            if not rsu_df.empty:
                merged_df = pd.concat([transactions_df, rsu_df], ignore_index=True)
                logger.info("数据合并完成，正在按交易时间重新排序...")
                return merged_df.sort_values(by='交易时间', ignore_index=True)
        except (FileNotFoundError, ValueError) as e:
            logger.error(f"处理RSU文件失败: {e}, 已跳过。")
    else:
        logger.info("未检测到 RSU 历史文件，跳过合并步骤。")
    return transactions_df

def calculate_tax(input_file: str, output_dir: str):
    """重构后的主计算函数。"""
    try:
        # 1. 加载主数据
        transactions_df = preprocess_data(input_file)

        # 2. (可选) 合并RSU数据
        input_dir = os.path.dirname(input_file)
        transactions_df = _merge_rsu_data(transactions_df, input_dir)

        # 3. 分类资产
        transactions_df['资产类型'] = transactions_df['股票代码'].apply(classify_asset)
        logger.info("数据加载和预处理完成。")

        # 4. 计算所有交易
        all_sales_df = _process_all_transactions(transactions_df)

        # 5. 生成并保存报告
        generate_and_save_reports(all_sales_df, output_dir)

        logger.info("处理完成。")

    except (FileNotFoundError, ValueError, KeyError) as e:
        logger.error(f"处理失败，发生错误: {e}")
    except Exception as e:
        logger.error(f"发生未知严重错误: {e}", exc_info=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='股票及期权年度报税计算器')
    default_csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'futu_history.csv')
    default_out_dir = os.path.join(os.path.dirname(__file__), '..', '税务报告')

    parser.add_argument('--input', type=str, default=default_csv_path, help='输入的CSV文件路径')
    parser.add_argument('--output', type=str, default=default_out_dir, help='输出报告的文件夹路径')
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    calculate_tax(args.input, args.output)
