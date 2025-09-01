# futu/download_history_flow.py
# Main script to download, process, and save historical deal data.

import logging
import sys
import os
from dotenv import load_dotenv

# Add the project root to the Python path to resolve module imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

# 导入重构后的公共模块
from futu_cli.common.connection import create_futu_connections, close_connections
from futu_cli.common.config import load_config

# 导入新的核心功能模块
from futu_cli.history.fetcher import fetch_history_deals
from futu_cli.history.fee_calculator import fetch_and_calculate_fees
from futu_cli.history.processor import process_history_data

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    主函数，编排历史成交数据的获取、处理和保存流程。
    """
    quote_ctx = None
    trade_ctx = None
    try:
        # 1. 加载配置并创建连接
        logger.info("========== 步骤 1/4: 初始化并创建连接 ==========")
        config = load_config('history_config')
        quote_ctx, trade_ctx = create_futu_connections()

        # 2. 获取原始成交数据
        logger.info("========== 步骤 2/4: 开始获取历史成交数据 ==========")
        deals_df = fetch_history_deals(trade_ctx, config)
        if deals_df.empty:
            logger.warning("未能获取到任何历史成交数据，程序终止。")
            return

        # 3. 获取并计算费用
        logger.info("========== 步骤 3/4: 开始获取并计算订单费用 ==========")
        deals_with_fees_df = fetch_and_calculate_fees(trade_ctx, deals_df, config)
        if deals_with_fees_df.empty:
            logger.warning("费用计算后数据为空，程序终止。")
            return
            
        # 4. 最终处理和保存
        logger.info("========== 步骤 4/4: 开始最终数据处理和保存 ==========")
        process_history_data(deals_with_fees_df, config)
        
        logger.info("历史成交数据处理任务全部完成！")

    except Exception as e:
        logger.error(f"执行历史成交数据下载任务失败: {e}", exc_info=True)
    finally:
        # 5. 安全关闭连接
        if quote_ctx and trade_ctx:
            logger.info("正在关闭富途连接...")
            close_connections(quote_ctx, trade_ctx)

if __name__ == '__main__':
    load_dotenv()
    main()