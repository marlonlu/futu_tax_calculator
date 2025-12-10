# download_cash_flow.py

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Generator, List

import pandas as pd
from futu import *

# 确保可以从同级目录导入
try:
    from futu_client import FutuClient
    from cached_trade_context import CachedTradeContext
    from rate_limiter import RateLimiter
except ImportError:
    print("错误：无法导入 futu_client、rate_limiter 或 cached_trade_context。请确保这些文件与 download_cash_flow.py 在同一目录中。")
    sys.exit(1)

# --- 配置日志记录 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# --- 数据获取 (Data Fetching Tier) ---

def _generate_daily_dates(start_date: datetime, end_date: datetime) -> Generator[datetime, None, None]:
    """生成一个日期范围内的每一天。"""
    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def fetch_cash_flow_by_day(
        trade_ctx,
        acc_id: int,
        start_date: datetime,
        end_date: datetime,
) -> Generator[pd.DataFrame, None, None]:
    """
    为单个账户逐日获取现金流记录，并提供详细的进度日志。
    这是一个生成器函数，每次API调用成功后返回一个 DataFrame。
    """
    date_range_str = f"{start_date.date()} 到 {end_date.date()}"
    logger.info(f"账户 {acc_id}: 开始查询 {date_range_str} 的现金流...")

    # --- 新增：为进度条计算总天数 ---
    total_days = (end_date - start_date).days + 1

    # --- 修改：在循环中加入计数和进度日志 ---
    for day_count, day in enumerate(_generate_daily_dates(start_date, end_date), 1):
        day_str = day.strftime('%Y-%m-%d')
        progress_percent = (day_count / total_days) * 100

        # --- 核心改进：输出带有明确进度的INFO级别日志 ---
        logger.info(
            f"[账户 {acc_id}] [进度 {day_count}/{total_days} ({progress_percent:.1f}%)] "
            f"正在查询日期: {day_str}..."
        )

        ret, data = trade_ctx.get_acc_cash_flow(
            clearing_date=day_str,
            acc_id=acc_id,
        )

        if ret != RET_OK:
            error_message = str(data)
            logger.error(
                f"API调用失败，流程将中断。账户: {acc_id}, 日期: {day_str}, 错误码: {ret}, 错误信息: {error_message}"
            )
            raise RuntimeError(
                f"Futu API call failed for account {acc_id} on date {day_str}. Reason: {error_message}"
            )

        if isinstance(data, pd.DataFrame) and not data.empty:
            # 修改：将成功获取的日志作为进度日志的补充信息
            logger.info(f"    -> 在 {day_str} 成功获取 {len(data)} 条现金流记录。")
            yield data

    logger.info(f"账户 {acc_id}: 查询完成。")


# --- 数据处理管道 (Data Processing Pipeline) ---
# 以下所有处理函数保持不变，因为它们只关心输入DataFrame的内容，不关心其获取方式。

def filter_dividend_cash_flow(df: pd.DataFrame) -> pd.DataFrame:
    """从现金流 DataFrame 中筛选出与股息相关的记录 (纯函数)。"""
    if df.empty:
        logger.warning("输入的DataFrame为空，直接返回。")
        return pd.DataFrame()
    logger.info(f"开始从 {len(df)} 条记录中筛选股息相关流水...")

    hk_dividend_pattern = '现金种子|现金股息|分红派息'
    is_hk_dividend = (
            (df['currency'].str.upper() == 'HKD') &
            (df['cashflow_type'].str.contains(hk_dividend_pattern, na=False,  regex=True))
    )
    us_dividend_pattern = 'SHARES DIVIDENDS|SHARES WITHHOLDING TAX'
    is_us_dividend_or_tax = (
            (df['currency'].str.upper() == 'USD') &
            (df['cashflow_remark'].str.contains(us_dividend_pattern, na=False, regex=True, case=False))
    )
    is_amount_not_zero = df['cashflow_amount'] != 0
    final_mask = (is_hk_dividend | is_us_dividend_or_tax) & is_amount_not_zero

    filtered_df = df[final_mask].copy()

    logger.info(f"筛选完毕，保留了 {len(filtered_df)} 条记录。")
    return filtered_df


def clean_and_sort_cash_flow(df: pd.DataFrame) -> pd.DataFrame:
    """对数据进行排序和去重 (纯函数)。"""
    if df.empty:
        return df
    sorted_df = df.sort_values(by='clearing_date', ascending=False, ignore_index=True)
    cleaned_df = sorted_df.drop_duplicates(subset=['cashflow_id'], keep='first', ignore_index=True)
    logger.info(f"排序和去重完成，处理前 {len(df)} 条，处理后 {len(cleaned_df)} 条。")
    return cleaned_df


def transform_to_output_format(df: pd.DataFrame) -> pd.DataFrame:
    """将最终数据转换为指定的输出格式 (纯函数)。"""
    if df.empty:
        return pd.DataFrame()
    return pd.DataFrame({
        'id': df['cashflow_id'],
        '金额': df['cashflow_amount'],
        '结算币种': df['currency'],
        '交易类型': df['cashflow_type'],
        '交易时间': df['clearing_date'],
        '交易备注': df['cashflow_remark'],
    })


# --- 文件保存 ---

def save_cash_flow_to_file(output_df: pd.DataFrame, output_filename: str) -> None:
    """将格式化后的数据保存到CSV文件。"""
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, output_filename)
    output_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    logger.info(f"股息现金流数据已导出到: {out_path}")


# --- 主流程与参数解析 (Application Entry Point) ---

def parse_arguments() -> argparse.Namespace:
    """解析、验证并返回命令行参数。"""
    today = datetime.now()
    default_start_date = datetime(today.year - 3, 1, 1)
    default_end_date = today

    parser = argparse.ArgumentParser(
        description='从 Futu OpenD 逐日下载并处理历史现金流数据，主要用于筛选股息。',
        formatter_class=argparse.RawTextHelpFormatter
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
        args.start_date_obj = datetime.strptime(args.start_date, '%Y-%m-%d')
        args.end_date_obj = datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError as e:
        parser.error(f"日期格式无效，请使用 'YYYY-MM-DD'。错误: {e}")
    if args.start_date_obj > args.end_date_obj:
        parser.error(f"开始日期 {args.start_date} 必须早于或等于结束日期 {args.end_date}。")
    return args


def run_cash_flow_download_flow(start_date: datetime, end_date: datetime):
    """
    执行完整的现金流数据下载和处理流程。
    """
    logger.info(f"开始执行现金流数据下载流程，时间范围: {start_date.date()} 到 {end_date.date()}")
    futu_client = FutuClient()
    rate_limiter = RateLimiter(max_requests=19, time_window=30)
    all_cash_flow_data: List[pd.DataFrame] = []

    try:
        _, raw_trade_ctx = futu_client.create_connections()
        trade_ctx = CachedTradeContext(raw_trade_ctx, rate_limiter=rate_limiter)
        valid_accounts = futu_client.get_valid_accounts(trade_ctx)

        logger.info(f"发现 {len(valid_accounts)} 个有效账户，将逐个查询现金流...")

        for _, acc_row in valid_accounts.iterrows():
            acc_id = int(acc_row['acc_id'])

            # 为此账户获取所有现金流数据
            cash_flow_generator = fetch_cash_flow_by_day(
                trade_ctx, acc_id, start_date, end_date
            )
            # 消耗生成器并将结果添加到总列表中
            all_cash_flow_data.extend(list(cash_flow_generator))

    except Exception as e:
        logger.error(f"在数据获取阶段发生错误: {e}", exc_info=True)
        return
    finally:
        logger.info("正在关闭 Futu API 连接...")
        futu_client.close_connections()

    if not all_cash_flow_data:
        logger.info("所有账户在指定的时间范围内未找到任何现金流记录。")
        return

    # --- 数据转换 (Transform) & 加载 (Load) 流程保持不变 ---
    logger.info("所有数据获取完毕，开始进行本地处理...")
    raw_df = pd.concat(all_cash_flow_data, ignore_index=True)

    # 保存原始流水，方便分析
    save_cash_flow_to_file(raw_df, 'futu_cash_flow_raw.csv')

    processed_df = (
        raw_df
        .pipe(filter_dividend_cash_flow)
        .pipe(clean_and_sort_cash_flow)
    )

    if processed_df.empty:
        logger.info("筛选后，没有符合条件的股息相关现金流记录。")
        return

    output_df = transform_to_output_format(processed_df)
    save_cash_flow_to_file(output_df, 'futu_cash_flow.csv')

    logger.info("流程执行完毕。")


if __name__ == '__main__':
    try:
        args = parse_arguments()
        run_cash_flow_download_flow(args.start_date_obj, args.end_date_obj)

        # 调试用
        # run_cash_flow_download_flow(
        #     datetime.strptime('2023-04-01', '%Y-%m-%d'),
        #     datetime.strptime('2023-06-09', '%Y-%m-%d')
        # )
    except Exception as e:
        logger.error(f"程序执行过程中发生未捕获的错误: {e}", exc_info=True)
