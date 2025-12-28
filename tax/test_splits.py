import pandas as pd
from datetime import datetime
import os

from stock_option_tax_calculator import (
    process_stock_transactions,
    load_splits_config,
    create_sales_record,
)


def make_txns(symbol, rows):
    """Helper to create a DataFrame with required columns from rows list of dicts."""
    df = pd.DataFrame(rows)
    # ensure required columns exist
    for col in ['交易时间', '数量', '成交价格', '合计手续费', '买卖方向', '股票代码', '结算币种']:
        if col not in df.columns:
            df[col] = None
    df['股票代码'] = symbol
    df['买卖方向'] = df['买卖方向'].fillna('buy')
    df['合计手续费'] = df['合计手续费'].fillna(0)
    return df


def test_split_forward_and_profit_adjustment(tmp_path):
    # create transactions: buy 100 @10 on 2024-01-01, split 2:1 on 2024-03-15, sell 150 @12 on 2024-04-01
    rows = [
        {'交易时间': '2024-01-01', '数量': 100, '成交价格': 10, '合计手续费': 0, '买卖方向': 'buy'},
        {'交易时间': '2024-04-01', '数量': 150, '成交价格': 12, '合计手续费': 0, '买卖方向': 'sell'},
    ]
    df = make_txns('FOO', rows)

    # create splits config file
    splits_csv = tmp_path / 'splits.csv'
    splits_csv.write_text('日期,股票代码,比例\n2024-03-15,FOO,2:1\n')

    splits_map = load_splits_config(str(splits_csv))
    sales = process_stock_transactions(df, 'FOO', splits_map.get('FOO'))

    # after split, holdings become 200 shares, cost_basis stays 100*10=1000, avg_cost per share = 5
    # selling 150 shares @12 => profit per share = 12 - 5 = 7 => total = 7*150 = 1050
    assert len(sales) == 1
    rec = sales[0]
    assert rec['数量'] == 150
    assert abs(rec['利润'] - 1050) < 1e-6


def test_reverse_split_and_cost_basis_preserved(tmp_path):
    # buy 100 @10 on 2024-01-01, reverse split 1:2 on 2024-06-30 -> holdings 50, cost_basis unchanged
    rows = [
        {'交易时间': '2024-01-01', '数量': 100, '成交价格': 10, '合计手续费': 0, '买卖方向': 'buy'},
        {'交易时间': '2024-07-01', '数量': 50, '成交价格': 30, '合计手续费': 0, '买卖方向': 'sell'},
    ]
    df = make_txns('BAR', rows)

    splits_csv = tmp_path / 'splits.csv'
    splits_csv.write_text('日期,股票代码,比例\n2024-06-30,BAR,1:2\n')

    splits_map = load_splits_config(str(splits_csv))
    sales = process_stock_transactions(df, 'BAR', splits_map.get('BAR'))

    # initial cost_basis = 100*10 = 1000, after reverse split holdings = 50, avg_cost per share = 1000/50 = 20
    # selling 50 @30 => profit = (30 - 20) * 50 = 500
    assert len(sales) == 1
    rec = sales[0]
    assert rec['数量'] == 50
    assert abs(rec['利润'] - 500) < 1e-6
