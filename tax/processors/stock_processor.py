from tax.data.preprocessor import is_buy, is_sell
from futu_cli.common.config import load_config


def _load_trading_config():
    """加载交易配置"""
    return load_config('trading_config')


def _clean_remark_text(text):
    """清理备注文本，过滤中英文逗号"""
    return text.replace(',', '').replace('，', '')


def _calculate_average_cost(holdings):
    """计算移动加权平均成本"""
    if holdings['quantity'] == 0:
        return 0
    return round(holdings['cost_basis'] / holdings['quantity'], 4)


def _process_buy_transaction(holdings, row):
    """处理买入交易"""
    total_cost = row['数量'] * row['成交价格'] + row['合计手续费']
    holdings['cost_basis'] += total_cost
    holdings['quantity'] += row['数量']


def _process_sell_transaction(holdings, row, code):
    """处理卖出交易，返回销售记录列表"""
    config = _load_trading_config()
    remarks = config.get('trading', {}).get('remarks', {})
    
    sales_records = []
    sold_quantity = row['数量']
    sale_price = row['成交价格']

    if holdings['quantity'] == 0:
        # 卖出时无持仓（可能是数据记录不全或卖空）
        remark = _clean_remark_text(remarks.get('no_position_on_sell', '卖出时无持仓 需手动核查'))
        sales_records.append(_create_sale_record(
            code, sale_price, 0, sold_quantity, 0, row, remark
        ))
        return sales_records

    avg_cost_per_share = _calculate_average_cost(holdings)
    actual_sold_quantity = min(sold_quantity, holdings['quantity'])

    if actual_sold_quantity > 0:
        profit = (sale_price - avg_cost_per_share) * actual_sold_quantity - row['合计手续费']
        sales_records.append(_create_sale_record(
            code, sale_price, avg_cost_per_share, actual_sold_quantity, profit, row, ''
        ))
        
        # 更新持仓
        holdings['cost_basis'] -= avg_cost_per_share * actual_sold_quantity
        holdings['quantity'] -= actual_sold_quantity

    # 处理卖超部分
    if sold_quantity > actual_sold_quantity:
        extra_quantity = sold_quantity - actual_sold_quantity
        remark = _clean_remark_text(remarks.get('oversell_position', '卖超持仓 需手动核查'))
        sales_records.append(_create_sale_record(
            code, sale_price, 0, extra_quantity, 0, row, remark
        ))
        # 将持仓清零
        holdings['quantity'] = 0
        holdings['cost_basis'] = 0

    return sales_records


def _create_sale_record(code, sale_price, cost_price, quantity, profit, row, note):
    """创建销售记录"""
    return {
        '股票代码': code,
        '卖出价格': sale_price,
        '成本价': cost_price,
        '数量': quantity,
        '利润': round(profit, 4),
        '时间': row['交易时间'],
        '结算币种': row['结算币种'],
        '备注': note
    }


def process_stock_transactions(df, code):
    """
    处理单个股票的完整历史交易记录。
    使用移动加权平均算法计算成本基础和利润。
    """
    # 确保数据按时间从早到晚排序（不信任外部排序）
    df_sorted = df.sort_values(by='交易时间', ascending=True).reset_index(drop=True)
    
    holdings = {'quantity': 0, 'cost_basis': 0.0}
    sales_records = []

    # 遍历按时间排序后的交易记录
    for index, row in df_sorted.iterrows():
        if is_buy(row):
            _process_buy_transaction(holdings, row)
        elif is_sell(row):
            sales_records.extend(_process_sell_transaction(holdings, row, code))

    return sales_records


def calculate_stock_holdings(df):
    """
    计算股票当前持仓状态
    """
    # 确保数据按时间从早到晚排序
    df_sorted = df.sort_values(by='交易时间', ascending=True).reset_index(drop=True)
    
    holdings = {'quantity': 0, 'cost_basis': 0.0}
    
    for index, row in df_sorted.iterrows():
        if is_buy(row):
            _process_buy_transaction(holdings, row)
        elif is_sell(row):
            if holdings['quantity'] > 0:
                avg_cost = _calculate_average_cost(holdings)
                sold_quantity = min(row['数量'], holdings['quantity'])
                holdings['cost_basis'] -= avg_cost * sold_quantity
                holdings['quantity'] -= sold_quantity
    
    return holdings