import re
import pandas as pd
import argparse
import os
from datetime import datetime, date


# --- 以下函数保持不变，因为它们的逻辑是正确的 ---

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
    # 步骤2: 执行替换操作，此时字典中的键应该都是小写
    replacement_map = {
        'orderside.sell': 'sell',
        'orderside.buy': 'buy',
        '卖出': 'sell',
        '买入': 'buy',
        'sell_short': 'sell',
        'buy_back': 'buy'
    }
    df['买卖方向'] = df['买卖方向'].replace(replacement_map)

    # 大模型经常出现买卖方向不同的关键词，这里打印下
    print("-" * 30)
    # 使用 value_counts() 统计每个 key 的出现次数
    key_counts = df['买卖方向'].value_counts()
    print("统计结果 (key: 个数):")
    # 遍历 Series 并以 'key: 个数' 格式打印
    for key, count in key_counts.items():
        print(f"{key}: {count}")

    # 移除关键数据列中的NaN值
    df.dropna(subset=['数量', '成交价格', '合计手续费', '交易时间'], inplace=True)

    # 按时间排序是保证后续计算正确性的关键
    df.sort_values(by='交易时间', inplace=True)

    return df


option_pattern = re.compile(r'US.([A-Z]+)(\d{6})([CP])(\d+)')


def classify_asset(code):
    """
    根据股票代码安全地区分资产类型。
    """
    if not isinstance(code, str):
        return 'Stock'
    if option_pattern.match(code):
        return 'Option'
    return 'Stock'


def is_buy(row):
    return str(row['买卖方向']).lower() == 'buy'


def is_sell(row):
    return str(row['买卖方向']).lower() == 'sell'


def process_stock_transactions(df, code):
    """
    处理单个股票的完整历史交易记录。
    """
    holdings = {'quantity': 0, 'cost_basis': 0.0}
    sales_records = []

    # 传入的 df 已经是按时间排序的单个资产的完整历史
    for index, row in df.iterrows():
        if is_buy(row):
            total_cost = row['数量'] * row['成交价格'] + row['合计手续费']
            holdings['cost_basis'] += total_cost
            holdings['quantity'] += row['数量']

        elif is_sell(row):
            sold_quantity = row['数量']
            sale_price = row['成交价格']

            if holdings['quantity'] == 0:
                # 卖出时无持仓（可能是数据记录不全或卖空）
                sales_records.append({
                    '股票代码': code, '卖出价格': sale_price, '成本价': 0,
                    '数量': sold_quantity, '利润': 0, '时间': row['交易时间'],
                    '结算币种': row['结算币种'], '备注': '卖出时无持仓, 需手动核查'
                })
                continue

            avg_cost_per_share = round(holdings['cost_basis'] / holdings['quantity'], 4)

            # 处理卖出数量超过持仓的情况
            actual_sold_quantity = min(sold_quantity, holdings['quantity'])

            if actual_sold_quantity > 0:
                profit = (sale_price - avg_cost_per_share) * actual_sold_quantity - row['合计手续费']
                sales_records.append({
                    '股票代码': code, '卖出价格': sale_price, '成本价': avg_cost_per_share,
                    '数量': actual_sold_quantity, '利润': round(profit, 4), '时间': row['交易时间'],
                    '结算币种': row['结算币种'], '备注': ''
                })
                holdings['cost_basis'] -= avg_cost_per_share * actual_sold_quantity
                holdings['quantity'] -= actual_sold_quantity

            if sold_quantity > actual_sold_quantity:
                # 卖超的部分
                extra_quantity = sold_quantity - actual_sold_quantity
                sales_records.append({
                    '股票代码': code, '卖出价格': sale_price, '成本价': 0,
                    '数量': extra_quantity, '利润': 0, '时间': row['交易时间'],
                    '结算币种': row['结算币种'], '备注': '卖超持仓, 需手动核查'
                })
                # 将持仓清零
                holdings['quantity'] = 0
                holdings['cost_basis'] = 0

    return sales_records


def is_expiration_future_or_current(expiration_date: datetime) -> bool:
    # 1. 获取当前日期
    current_date = date.today()
    # 2. 提取 expiration_date 的日期部分
    #    虽然 expiration_date 已经是一个 datetime 对象，但它的时间部分是00:00:00
    #    为了和 current_date (只有日期没有时间) 公平比较，我们只取它的日期部分
    parsed_date = expiration_date.date()
    # 3. 进行比较
    return parsed_date >= current_date


def extract_expiration_date(option_code):
    """从期权代码中解析出到期日"""
    match = option_pattern.match(option_code)
    if match:
        date_str = match.group(2)  # 形如 "231229"
        try:
            # %y 会智能处理20世纪和21世纪的年份
            # 例如 '23' -> 2023, '99' -> 1999
            expiration_date = datetime.strptime(date_str, '%y%m%d')
            return expiration_date
        except ValueError:
            return None
    return None


def process_option_transactions(df, code):
    """
    处理单个期权的完整历史交易记录，使用更精确的空头头寸追踪方法。
    """
    # 状态变量:
    # quantity: 正数=多头持仓, 负数=空头持仓
    # cost_basis: 多头头寸的总成本 (买入价 * 数量 * 乘数 + 手续费)
    # short_proceeds: 空头头寸的总收入 (卖出价 * 数量 * 乘数 - 手续费)
    holdings = {'quantity': 0, 'cost_basis': 0.0, 'short_proceeds': 0.0}
    sales_records = []
    price_multiplier = 100

    # 1. 解析期权到期日
    expiration_date = extract_expiration_date(code)
    if not expiration_date:
        expiration_date = df['交易时间'].iloc[-1]
        expiration_note = '到期日无法解析，使用最后交易日代替'
    else:
        expiration_note = ''

    # 2. 遍历所有交易
    for index, row in df.iterrows():
        qty = row['数量']
        price = row['成交价格']
        fee = row['合计手续费']

        if is_buy(row):  # 买入操作 (Buy to Open 或 Buy to Close)
            if holdings['quantity'] < 0:  # -> 买入平仓 (Buy to Close)
                # 当前持有空头仓位
                close_quantity = min(qty, abs(holdings['quantity']))

                # 计算空头头寸的平均开仓价格
                # 这个价格代表了之前卖空时，平均每份合约收到的净收入
                avg_short_price_per_contract = holdings['short_proceeds'] / (
                            abs(holdings['quantity']) * price_multiplier)

                # 平仓成本
                close_cost = close_quantity * price * price_multiplier + fee
                # 平仓对应的开仓收入
                proceeds_to_close = close_quantity * avg_short_price_per_contract * price_multiplier

                profit = proceeds_to_close - close_cost

                sales_records.append({
                    '股票代码': code,
                    '卖出价格': round(avg_short_price_per_contract, 4),  # 对应开仓时的"卖出价"
                    '成本价': price,  # 本次平仓的"成本价"
                    '数量': close_quantity,
                    '利润': round(profit, 4),
                    '时间': row['交易时间'],
                    '结算币种': row['结算币种'],
                    '备注': '卖空平仓'
                })

                # 更新持仓状态
                holdings['quantity'] += close_quantity
                holdings['short_proceeds'] -= proceeds_to_close

                # 如果买入数量大于平仓所需数量，剩余部分视为开多仓
                if qty > close_quantity:
                    open_quantity = qty - close_quantity
                    holdings['cost_basis'] += open_quantity * price * price_multiplier  # 手续费已在平仓时计算
                    holdings['quantity'] += open_quantity

            else:  # -> 买入开仓 (Buy to Open)
                total_cost = qty * price * price_multiplier + fee
                holdings['cost_basis'] += total_cost
                holdings['quantity'] += qty

        elif is_sell(row):  # 卖出操作 (Sell to Close 或 Sell to Open)
            if holdings['quantity'] > 0:  # -> 卖出平仓 (Sell to Close)
                # 当前持有多头仓位
                sell_quantity = min(qty, holdings['quantity'])

                # 计算多头头寸的平均成本价
                avg_long_cost_per_contract = holdings['cost_basis'] / holdings['quantity'] / price_multiplier

                # 平仓收入
                sale_proceeds = sell_quantity * price * price_multiplier - fee
                # 平仓对应的成本
                cost_to_close = sell_quantity * avg_long_cost_per_contract * price_multiplier

                profit = sale_proceeds - cost_to_close

                sales_records.append({
                    '股票代码': code,
                    '卖出价格': price,
                    '成本价': round(avg_long_cost_per_contract, 4),
                    '数量': sell_quantity,
                    '利润': round(profit, 4),
                    '时间': row['交易时间'],
                    '结算币种': row['结算币种'],
                    '备注': ''
                })

                # 更新持仓状态
                holdings['quantity'] -= sell_quantity
                holdings['cost_basis'] -= cost_to_close

                # 如果卖出数量大于平仓所需数量，剩余部分视为开空仓
                if qty > sell_quantity:
                    open_quantity = qty - sell_quantity
                    holdings['short_proceeds'] += open_quantity * price * price_multiplier  # 手续费已在平仓时计算
                    holdings['quantity'] -= open_quantity

            else:  # -> 卖出开仓 (Sell to Open)
                total_proceeds = qty * price * price_multiplier - fee
                holdings['short_proceeds'] += total_proceeds
                holdings['quantity'] -= qty

    if is_expiration_future_or_current(expiration_date):  # 未到期，直接忽略
        return sales_records

    # 3. 期末处理未平仓头寸
    if holdings['quantity'] > 0:
        # 场景一：多头头寸剩余，视为到期作废 (Expired worthless)
        # 利润就是全部的剩余成本（负数）
        profit = -holdings['cost_basis']
        avg_cost = holdings['cost_basis'] / holdings['quantity'] / price_multiplier

        sales_records.append({
            '股票代码': code, '卖出价格': 0, '成本价': round(avg_cost, 4),
            '数量': holdings['quantity'], '利润': round(profit, 4),
            '时间': expiration_date, '结算币种': df['结算币种'].iloc[-1],
            '备注': f'期权到期作废 {expiration_note}'.strip()
        })
    elif holdings['quantity'] < 0:
        # 场景二：空头头寸剩余，视为到期盈利 (Seller keeps the premium)
        # 相当于以 0 成本买回归零，利润就是剩余的`short_proceeds`
        remaining_quantity = abs(holdings['quantity'])
        profit = holdings['short_proceeds']
        avg_sell_price = holdings['short_proceeds'] / remaining_quantity / price_multiplier

        sales_records.append({
            '股票代码': code,
            '卖出价格': round(avg_sell_price, 4),  # 名义上的平均卖出价
            '成本价': 0,  # 以0成本买回
            '数量': remaining_quantity,
            '利润': round(profit, 4),
            '时间': expiration_date,
            '结算币种': df['结算币种'].iloc[-1],
            '备注': f'卖空期权到期 {expiration_note}'.strip()
        })

    return sales_records

# --- 以下是重构后的核心逻辑 ---

def generate_and_save_reports(df, output_dir):
    """
    处理所有交易，生成报告，并按年份和币种分别添加汇总行。
    """
    all_sales_records = []

    # 1. 按“股票代码”分组处理，生成所有交易的盈亏记录
    for code, group_df in df.groupby('股票代码'):
        asset_type = group_df['资产类型'].iloc[0]

        print(f"正在处理资产: {code} ({asset_type})")

        if asset_type == 'Stock':
            stock_results = process_stock_transactions(group_df, code)
            all_sales_records.extend(stock_results)
        elif asset_type == 'Option':
            # 确保将 `process_option_transactions` 的最新版本放在这里
            option_results = process_option_transactions(group_df, code)
            all_sales_records.extend(option_results)

    # 2. 检查是否有记录
    if not all_sales_records:
        return False

    # 3. 创建总报告DataFrame
    report_df = pd.DataFrame(all_sales_records)
    report_df['时间'] = pd.to_datetime(report_df['时间'])
    report_df['年份'] = report_df['时间'].dt.year

    # 4. 按“年份”分割报告，并为每个年份生成按币种的汇总
    for year, year_df in report_df.groupby('年份'):

        # --- 全新的、按币种汇总的逻辑 ---

        # 创建一个列表来收集该年度所有币种的汇总行
        yearly_summary_rows = []

        # 在年度数据内，再按“结算币种”进行分组
        for currency, currency_df in year_df.groupby('结算币种'):
            # 计算当前币种的总计
            total_profit = currency_df['利润'].sum()

            # 识别期权并计算正确的交易金额
            is_option = currency_df['股票代码'].str.match(option_pattern)
            multiplier = is_option.apply(lambda x: 100 if x else 1)

            total_sales_value = (currency_df['卖出价格'] * currency_df['数量'] * multiplier).sum()
            total_cost_value = (currency_df['成本价'] * currency_df['数量'] * multiplier).sum()

            # 构建当前币种的汇总行
            summary_time = datetime(year, 12, 31, 23, 59, 59)
            total_fee = abs(abs(total_sales_value - total_cost_value) - abs(total_profit))
            total_fee = round(total_fee, 2)
            summary_row = {
                '股票代码': f'{year}年度汇总 ({currency})',  # 股票代码中包含币种
                '卖出价格': total_sales_value,
                '成本价': total_cost_value,
                '数量': 0,
                '利润': total_profit,
                '时间': summary_time,
                '结算币种': currency,  # 明确币种
                '备注': f'盈利已剔除手续费 {total_fee} ({currency})'
            }
            yearly_summary_rows.append(summary_row)  # 将此币种的汇总行添加到列表中

        # 如果有汇总行，则进行合并
        if yearly_summary_rows:
            summary_df = pd.DataFrame(yearly_summary_rows)
            # 将所有汇总行添加到年度报告的末尾
            final_report_df = pd.concat([year_df, summary_df], ignore_index=True)
        else:
            final_report_df = year_df  # 如果该年没有任何交易，则直接使用原df

        # --- 汇总逻辑结束 ---

        # 准备输出
        final_report_df = final_report_df.sort_values(by='时间', ascending=True)
        final_report_df = final_report_df[['股票代码', '卖出价格', '成本价', '数量', '利润', '时间', '结算币种', '备注']]

        output_filename = os.path.join(output_dir, f"{year}_report.csv")
        final_report_df.to_csv(output_filename, index=False, encoding='utf-8-sig', float_format='%.4f')
        print(f"税务报告已保存至: {output_filename}")

    return True


def calculate_tax(input_file, output_dir):
    """
    重构后的主计算函数
    """
    # 步骤 1: 加载和预处理数据
    transactions_df = preprocess_data(input_file)

    # 1. 构建 RSU 文件的预期路径
    input_dir = os.path.dirname(input_file)
    rsu_file_path = os.path.join(input_dir, 'futu_rsu_history.csv')
    # 2. 检查文件是否存在
    if os.path.exists(rsu_file_path):
        print(f"检测到 RSU 历史文件: {rsu_file_path}, 开始合并处理...")

        # 3. 加载并预处理 RSU 数据
        rsu_df = preprocess_data(rsu_file_path)
        if rsu_df is not None and not rsu_df.empty:
            # 4. 合并两个 DataFrame
            print("正在合并主交易数据与 RSU 数据...")
            transactions_df = pd.concat([transactions_df, rsu_df], ignore_index=True)

            # 5. 对合并后的数据全局按时间排序
            print("正在按交易时间重新排序所有记录...")
            transactions_df.sort_values(by='交易时间', inplace=True, ignore_index=True)
            print("数据合并与排序完成。")
    else:
        print("未检测到 RSU 历史文件，跳过合并步骤。")

    # 步骤 2: 区分资产类型
    transactions_df['资产类型'] = transactions_df['股票代码'].apply(classify_asset)
    print("数据加载和预处理完成。")

    # 步骤 3: 根据新逻辑生成并保存报告
    reports_were_generated = generate_and_save_reports(transactions_df, output_dir)

    if reports_were_generated:
        print(f"\n处理完成，年度报告已保存在目录: {output_dir}")
    else:
        print("\n处理完成，但没有发现任何可报告的卖出交易，因此未生成任何报告。")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='股票及期权年度报税计算器')
    parser.add_argument('--input', type=str, default='data/futu_history.csv', help='输入的CSV文件路径')
    parser.add_argument('--output', type=str, default='税务报告', help='输出报告的文件夹路径')
    args = parser.parse_args()

    # 确保输出目录存在
    output_dir = args.output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    calculate_tax(args.input, args.output)
