from tax.data.preprocessor import is_buy, is_sell
from tax.utils.option_parser import extract_expiration_date, is_expiration_future_or_current


def process_option_transactions(df, code):
    """
    处理单个期权的完整历史交易记录，使用更精确的空头头寸追踪方法。
    """
    # 确保数据按时间从早到晚排序（不信任外部排序）
    df_sorted = df.sort_values(by='交易时间', ascending=True).reset_index(drop=True)
    
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
        expiration_date = df_sorted['交易时间'].iloc[-1]
        expiration_note = '到期日无法解析 使用最后交易日代替'
    else:
        expiration_note = ''

    # 2. 遍历按时间排序后的交易记录
    for index, row in df_sorted.iterrows():
        qty = row['数量']
        price = row['成交价格']
        fee = row['合计手续费']

        if is_buy(row):  # 买入操作 (Buy to Open 或 Buy to Close)
            sales_records.extend(_process_buy_transaction(
                holdings, qty, price, fee, row, code, price_multiplier
            ))

        elif is_sell(row):  # 卖出操作 (Sell to Close 或 Sell to Open)
            sales_records.extend(_process_sell_transaction(
                holdings, qty, price, fee, row, code, price_multiplier
            ))

    # 3. 处理期权到期的未平仓头寸
    currency = df_sorted['结算币种'].iloc[-1] if not df_sorted.empty else 'USD'
    expiration_records = handle_option_expiration(
        holdings, code, expiration_date, expiration_note, currency
    )
    
    # 4. 合并交易记录和到期记录
    all_records = sales_records + expiration_records
    
    return all_records, holdings, expiration_date, expiration_note


def handle_option_expiration(holdings, code, expiration_date, expiration_note, currency):
    """
    处理期权到期的未平仓头寸
    """
    expiration_records = []
    
    if is_expiration_future_or_current(expiration_date):  # 未到期，直接忽略
        return expiration_records

    price_multiplier = 100
    
    if holdings['quantity'] > 0:
        # 场景一：多头头寸剩余，视为到期作废 (Expired worthless)
        # 利润就是全部的剩余成本（负数）
        profit = -holdings['cost_basis']
        avg_cost = holdings['cost_basis'] / holdings['quantity'] / price_multiplier

        expiration_records.append({
            '股票代码': code, '卖出价格': 0, '成本价': round(avg_cost, 4),
            '数量': holdings['quantity'], '利润': round(profit, 4),
            '时间': expiration_date, '结算币种': currency,
            '备注': f'到期作废 1手=100股 {expiration_note}'.strip()
        })
        
    elif holdings['quantity'] < 0:
        # 场景二：空头头寸剩余，视为到期盈利 (Seller keeps the premium)
        # 卖空期权到期，保留全部权利金收入作为利润
        remaining_quantity = abs(holdings['quantity'])
        profit = holdings['short_proceeds']  # 权利金收入已扣除手续费
        avg_sell_price = holdings['short_proceeds'] / remaining_quantity / price_multiplier

        expiration_records.append({
            '股票代码': code,
            '卖出价格': round(avg_sell_price, 4),  # 原始卖出价格
            '成本价': 0,  # 到期时以0成本平仓
            '数量': remaining_quantity,
            '利润': round(profit, 4),
            '时间': expiration_date,
            '结算币种': currency,
            '备注': f'到期作废 1手=100股 {expiration_note}'.strip()
        })

    return expiration_records


def process_option_with_expiration(df, code):
    """
    处理期权交易并包含到期处理
    """
    # 确保数据按时间从早到晚排序
    df_sorted = df.sort_values(by='交易时间', ascending=True).reset_index(drop=True)
    
    # 直接调用 process_option_transactions，它已经包含了到期处理
    all_records, holdings, expiration_date, expiration_note = process_option_transactions(df_sorted, code)
    
    return all_records, holdings, expiration_date, expiration_note


def _process_buy_transaction(holdings, qty, price, fee, row, code, price_multiplier):
    """处理买入交易"""
    sales_records = []
    
    if holdings['quantity'] < 0:  # -> 买入平仓 (Buy to Close)
        # 当前持有空头仓位
        close_quantity = min(qty, abs(holdings['quantity']))

        # 计算空头头寸的平均开仓价格
        avg_short_price_per_contract = holdings['short_proceeds'] / (
                    abs(holdings['quantity']) * price_multiplier)

        # 平仓成本和收入
        close_cost = close_quantity * price * price_multiplier + fee
        proceeds_to_close = close_quantity * avg_short_price_per_contract * price_multiplier
        profit = proceeds_to_close - close_cost

        sales_records.append({
            '股票代码': code,
            '卖出价格': round(avg_short_price_per_contract, 4),
            '成本价': price,
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
            holdings['cost_basis'] += open_quantity * price * price_multiplier
            holdings['quantity'] += open_quantity

    else:  # -> 买入开仓 (Buy to Open)
        total_cost = qty * price * price_multiplier + fee
        holdings['cost_basis'] += total_cost
        holdings['quantity'] += qty
    
    return sales_records


def _process_sell_transaction(holdings, qty, price, fee, row, code, price_multiplier):
    """处理卖出交易"""
    sales_records = []
    
    if holdings['quantity'] > 0:  # -> 卖出平仓 (Sell to Close)
        # 当前持有多头仓位
        sell_quantity = min(qty, holdings['quantity'])

        # 计算多头头寸的平均成本价
        avg_long_cost_per_contract = holdings['cost_basis'] / holdings['quantity'] / price_multiplier

        # 平仓收入和成本
        sale_proceeds = sell_quantity * price * price_multiplier - fee
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
            holdings['short_proceeds'] += open_quantity * price * price_multiplier
            holdings['quantity'] -= open_quantity

    else:  # -> 卖出开仓 (Sell to Open)
        total_proceeds = qty * price * price_multiplier - fee
        holdings['short_proceeds'] += total_proceeds
        holdings['quantity'] -= qty
    
    return sales_records