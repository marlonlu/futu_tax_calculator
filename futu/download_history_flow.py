import argparse
import itertools
import logging
import os
from datetime import datetime, timedelta
from typing import Generator, Iterable, Tuple

import pandas as pd
from futu import *
from futu_client import FutuClient
from rate_limiter import RateLimiter

# --- 配置日志记录 ---
# 使用日志模块替代 print，便于区分信息、警告和错误
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# --- 数据获取 (Data Fetching Tier) ---

def _generate_date_chunks(start_date: datetime, end_date: datetime, days_per_chunk: int = 90) -> Generator[Tuple[datetime, datetime], None, None]:
    """
    将一个大的时间范围切分成多个小的时间块。

    Args:
        start_date: 开始时间。
        end_date: 结束时间。
        days_per_chunk: 每个时间块的天数。

    Yields:
        一个包含 (块开始时间, 块结束时间) 的元组。
    """
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=days_per_chunk), end_date)
        yield current_start, current_end
        current_start = current_end


def query_deals_by_date_range(trade_ctx, acc_id: int, market, rate_limiter: RateLimiter, start_date: datetime, end_date: datetime) -> Generator[pd.DataFrame, None, None]:
    """
    按时间范围分批查询成交记录，并作为生成器返回。

    Args:
        trade_ctx: 交易上下文对象。
        acc_id: 账户ID。
        market: 市场类型。
        rate_limiter: 请求限制器。
        start_date: 查询开始时间。
        end_date: 查询结束时间。

    Yields:
        每次API调用成功后返回的成交记录 DataFrame。
    """
    for start_chunk, end_chunk in _generate_date_chunks(start_date, end_date):
        logger.info(f"账户 {acc_id}: 正在获取 {start_chunk.strftime('%Y-%m-%d')} 到 {end_chunk.strftime('%Y-%m-%d')} 的成交数据...")
        rate_limiter.wait_if_needed()

        ret, data = trade_ctx.history_deal_list_query(
            acc_id=acc_id,
            deal_market=market,
            start=start_chunk.strftime('%Y-%m-%d %H:%M:%S'),
            end=end_chunk.strftime('%Y-%m-%d %H:%M:%S'),
        )

        if ret != RET_OK:
            raise Exception(f'获取历史成交失败: {data}')

        if isinstance(data, pd.DataFrame) and not data.empty:
            data['acc_id'] = acc_id
            logger.info(f"    成功获取 {len(data)} 条成交记录。")
            yield data


def fetch_all_deals_for_account(trade_ctx, acc_row: pd.Series, markets: list, rate_limiter: RateLimiter, start_date: datetime, end_date: datetime) -> Iterable[pd.DataFrame]:
    """
    查询单个账户在指定市场范围内的所有成交记录。

    Args:
        trade_ctx: 交易上下文对象。
        acc_row: 包含账户信息的 Pandas Series。
        markets: 要查询的市场列表。
        rate_limiter: 请求限制器。
        start_date: 查询开始时间。
        end_date: 查询结束时间。

    Returns:
        一个包含该账户所有成交记录 DataFrame 的可迭代对象。
    """
    try:
        acc_id = int(acc_row['acc_id'])
        logger.info(f"开始处理账户 ID: {acc_id} (牛牛号: {acc_row.get('uni_card_num', 'N/A')})")
    except (ValueError, TypeError):
        logger.warning(f"无效的账户ID: {acc_row.get('acc_id')}, 跳过此账户。")
        return []

    # 使用 itertools.chain.from_iterable 将多层嵌套的生成器扁平化
    return itertools.chain.from_iterable(
        query_deals_by_date_range(trade_ctx, acc_id, market, rate_limiter, start_date, end_date)
        for market in markets
    )


def _fetch_fees_for_account(trade_ctx, acc_id: int, order_ids: list, batch_size: int = 400) -> list:
    """内部函数，为单个账户批量查询订单费用。"""
    fee_dataframes = []
    for i in range(0, len(order_ids), batch_size):
        batch_ids = order_ids[i:i+batch_size]
        ret, fee_df = trade_ctx.order_fee_query(
            order_id_list=batch_ids,
            acc_id=acc_id,
            trd_env=TrdEnv.REAL
        )
        if ret == RET_OK and isinstance(fee_df, pd.DataFrame):
            fee_dataframes.append(fee_df[['order_id', 'fee_amount']])
        else:
            logger.warning(f'账户 {acc_id} 获取批次订单费用失败: {fee_df}')
    return fee_dataframes


def fetch_all_order_fees(trade_ctx, deals_df: pd.DataFrame) -> pd.DataFrame:
    """
    为所有成交记录批量查询相关订单的费用。

    Args:
        trade_ctx: 交易上下文对象。
        deals_df: 包含所有成交记录的 DataFrame。

    Returns:
        一个包含 'order_id' 和 'fee_amount' 的费用 DataFrame。
    """
    if 'order_id' not in deals_df.columns or 'acc_id' not in deals_df.columns:
        logger.warning("输入的DataFrame缺少 'order_id' 或 'acc_id' 列，无法查询费用。")
        return pd.DataFrame(columns=['order_id', 'fee_amount'])

    all_fees = []
    # 按账户分组查询，提高效率
    for acc_id_val, group in deals_df.groupby('acc_id'):
        try:
            acc_id_int = int(acc_id_val)
            order_ids = group['order_id'].unique().tolist()
            account_fees = _fetch_fees_for_account(trade_ctx, acc_id_int, order_ids)
            all_fees.extend(account_fees)
        except (ValueError, TypeError):
            logger.warning(f'无法转换 acc_id: {acc_id_val} 为整数，跳过该账户的费用查询。')
            continue

    if not all_fees:
        return pd.DataFrame(columns=['order_id', 'fee_amount'])

    # 合并并去重，因为不同账户可能查询到相同的订单ID（虽然不太可能，但作为保险）
    return (
        pd.concat(all_fees, ignore_index=True)
        .groupby('order_id', as_index=False)
        .first()
    )


# --- 数据处理管道 (Data Processing Pipeline) ---

def remove_duplicate_deals(deals_df: pd.DataFrame) -> pd.DataFrame:
    """
    移除重复的成交记录并记录日志 (纯函数版本)。

    Args:
        deals_df: 包含成交记录的 DataFrame。

    Returns:
        移除重复项后的新 DataFrame。
    """
    duplicate_cols = ['order_id', 'qty', 'price', 'trd_side', 'create_time']
    # 找出所有重复的记录（除了第一次出现的）
    duplicates = deals_df[deals_df.duplicated(subset=duplicate_cols, keep=False)]
    # 找出要被移除的记录
    rows_to_remove = deals_df[deals_df.duplicated(subset=duplicate_cols, keep='first')]

    if not rows_to_remove.empty:
        logger.info(f"发现 {len(rows_to_remove)}/{len(duplicates)} 条重复的成交记录将被移除。详情如下:")
        logger.info(f"\n{duplicates.sort_values(by=duplicate_cols).to_string()}")

    return deals_df.drop_duplicates(subset=duplicate_cols, keep='first', ignore_index=True)


def merge_and_distribute_fees(deals_df: pd.DataFrame, fees_df: pd.DataFrame) -> pd.DataFrame:
    """
    将费用合并到成交记录中，并处理组合订单的费用分配 (纯函数版本)。

    Args:
        deals_df: 成交记录 DataFrame。
        fees_df: 费用 DataFrame。

    Returns:
        合并费用并正确分配后的新 DataFrame。
    """
    if not fees_df.empty:
        merged_df = deals_df.merge(fees_df, on='order_id', how='left')
        merged_df.rename(columns={'fee_amount': '合计手续费'}, inplace=True)
        merged_df['合计手续费'].fillna(0, inplace=True)
    else:
        merged_df = deals_df.copy()
        merged_df['合计手续费'] = 0

    # 对于组合策略（同一order_id有多条成交），只在第一条记录上保留费用
    # 1. 确保排序以获得一致的结果
    sorted_df = merged_df.sort_values(by=['order_id', 'create_time'], ignore_index=True)
    # 2. 标记每个 order_id 组中除了第一条以外的所有记录
    mask = sorted_df.duplicated(subset='order_id', keep='first')
    # 3. 将这些记录的费用设置为0
    sorted_df.loc[mask, '合计手续费'] = 0

    return sorted_df


def sort_and_clean_final_data(deals_df: pd.DataFrame) -> pd.DataFrame:
    """
    对最终数据进行排序和最终去重 (纯函数版本)。

    Args:
        deals_df: 待处理的 DataFrame。

    Returns:
        处理完毕的新 DataFrame。
    """
    # 重新按创建时间排序，并重置索引
    sorted_df = deals_df.sort_values(by='create_time', ascending=True, ignore_index=True)

    # 再次去重，作为最终检查
    logger.info("进行最终的数据一致性检查（去重）...")
    cleaned_df = remove_duplicate_deals(sorted_df)

    return cleaned_df


def transform_to_output_format(final_df: pd.DataFrame) -> pd.DataFrame:
    """
    将最终处理过的 DataFrame 转换为指定的输出格式 (纯函数版本)。

    Args:
        final_df: 最终的内部数据 DataFrame。

    Returns:
        符合输出要求的 DataFrame。
    """
    return pd.DataFrame({
        '股票代码': final_df['code'],
        '数量': final_df['qty'],
        '成交价格': final_df['price'],
        '买卖方向': final_df['trd_side'].replace({'BUY': 'buy', 'SELL': 'sell'}),
        '结算币种': final_df['deal_market'].replace({'HK': 'HKD', 'US': 'USD'}),
        '合计手续费': final_df['合计手续费'],
        '交易时间': final_df['create_time'].str[:19],  # 截断毫秒
    })


# --- 文件保存 ---

def save_data_to_files(raw_df: pd.DataFrame, output_df: pd.DataFrame) -> None:
    """
    将原始数据和格式化后的数据保存到CSV文件。

    Args:
        raw_df: 包含所有原始字段的最终 DataFrame。
        output_df: 格式化后的输出 DataFrame。
    """
    # 创建 'data' 目录（如果不存在）
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(output_dir, exist_ok=True)

    raw_path = os.path.join(output_dir, 'futu_history_raw.csv')
    raw_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
    logger.info(f"所有账户的原始数据已合并保存到: {raw_path}")

    out_path = os.path.join(output_dir, 'futu_history.csv')
    output_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    logger.info(f"格式化后的数据已导出到: {out_path}")


# --- 主流程与参数解析 (Application Entry Point) ---

def parse_arguments() -> argparse.Namespace:
    """
    解析、验证并返回命令行参数。

    Returns:
        一个包含 `start_date` 和 `end_date` (datetime 对象) 的命名空间。
    """
    current_year = datetime.now().year
    default_end_date = datetime(current_year, 1, 1)
    default_start_date = default_end_date - timedelta(days=int(365 * 3.5))

    parser = argparse.ArgumentParser(
        description='从 Futu OpenD 下载并处理历史成交数据。',
        formatter_class=argparse.RawTextHelpFormatter,
        epilog='''示例用法:
  # 使用默认时间范围 (最近3.5年，截止到今年1月1日)
  python -m futu.download_history_flow

  # 自定义时间范围
  python -m futu.download_history_flow --start-date 2022-01-01 --end-date 2023-12-31
'''
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=default_start_date.strftime('%Y-%m-%d'),
        help=f"查询开始日期 (格式: YYYY-MM-DD)。默认为: {default_start_date.strftime('%Y-%m-%d')}"
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=default_end_date.strftime('%Y-%m-%d'),
        help=f"查询结束日期 (格式: YYYY-MM-DD)。默认为: {default_end_date.strftime('%Y-%m-%d')}"
    )
    args = parser.parse_args()

    try:
        args.start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        args.end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        parser.error(f"日期格式无效，请使用 'YYYY-MM-DD'。错误: {e}")

    if args.start_date >= args.end_date:
        parser.error(f"开始日期 {args.start_date.date()} 必须早于结束日期 {args.end_date.date()}。")

    return args


def run_download_flow(start_date: datetime, end_date: datetime):
    """
    执行完整的数据下载和处理流程。

    Args:
        start_date: 查询开始时间。
        end_date: 查询结束时间。
    """
    logger.info(f"开始执行历史数据下载流程，时间范围: {start_date.date()} 到 {end_date.date()}")
    futu_client = FutuClient()
    rate_limiter = RateLimiter(max_requests=9, time_window=30)
    # 注意: TrdMarket.NONE 表示查询所有市场的成交记录，这是正确的用法
    markets_to_query = [TrdMarket.NONE]

    try:
        quote_ctx, trade_ctx = futu_client.create_connections()
        valid_accounts = futu_client.get_valid_accounts(trade_ctx)

        # --- 1. 数据提取 (Extract) ---
        all_deals_iter = itertools.chain.from_iterable(
            fetch_all_deals_for_account(trade_ctx, acc_row, markets_to_query, rate_limiter, start_date, end_date)
            for _, acc_row in valid_accounts.iterrows()
        )
        all_deals_df = pd.concat(all_deals_iter, ignore_index=True)

        if all_deals_df.empty:
            logger.info("在指定的时间范围内未找到任何成交记录。")
            return

        # 提取费用
        fees_df = fetch_all_order_fees(trade_ctx, all_deals_df)

    finally:
        logger.info("正在关闭 Futu API 连接...")
        futu_client.close_connections()

    # --- 2. 数据转换 (Transform) ---
    logger.info("所有数据获取完毕，开始进行本地处理...")
    # 应用数据处理管道
    processed_df = (
        all_deals_df
        .pipe(remove_duplicate_deals)
        .pipe(merge_and_distribute_fees, fees_df=fees_df)
        .pipe(sort_and_clean_final_data)
    )

    output_df = transform_to_output_format(processed_df)

    # --- 3. 数据加载 (Load) ---
    save_data_to_files(raw_df=processed_df, output_df=output_df)
    logger.info("流程执行完毕。")


if __name__ == '__main__':
    try:
        args = parse_arguments()
        run_download_flow(args.start_date, args.end_date)

        # 调试用
        # run_download_flow(
        #     datetime.strptime('2023-04-01', '%Y-%m-%d'),
        #     datetime.strptime('2023-06-09', '%Y-%m-%d')
        # )
    except Exception as e:
        logger.error(f"程序执行过程中发生未捕获的错误: {e}", exc_info=True)
        # exc_info=True 会在日志中记录完整的堆栈跟踪信息，非常适合调试

