# futu/download_cash_flow.py
# Main script to download, process, and save cash flow data.

import logging
import pandas as pd
from futu.common.config import load_config
from futu.cash_flow.fetcher import fetch_cash_flow
from futu.cash_flow.filter import filter_cash_flow_data
from futu.cash_flow.processor import process_cash_flow_data

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """
    主函数，编排现金流数据的获取、处理和保存流程。
    """
    try:
        # 0. 预先加载所有需要的配置
        logger.info("========== 步骤 0/4: 加载配置文件 ==========")
        conn_config = load_config('connection_config')
        cash_flow_config = load_config('cash_flow_config')
        
        # 1. 获取原始数据
        logger.info("========== 步骤 1/4: 开始获取现金流数据 ==========")
        raw_data = fetch_cash_flow(conn_config, cash_flow_config)
        if raw_data.empty:
            logger.warning("未能获取到任何现金流数据，程序终止。")
            return

        # 2. 过滤数据 (当前为占位)
        logger.info("========== 步骤 2/4: 开始过滤现金流数据 ==========")
        filtered_data = filter_cash_flow_data(raw_data, cash_flow_config)
        if filtered_data.empty:
            logger.warning("数据过滤后为空，程序终止。")
            return

        # 3. 处理数据
        logger.info("========== 步骤 3/4: 开始处理现金流数据 ==========")
        processed_data = process_cash_flow_data(filtered_data)
        if processed_data.empty:
            logger.warning("数据处理后为空，程序终止。")
            return

        # 4. 保存结果
        logger.info("========== 步骤 4/4: 开始保存处理结果 ==========")
        output_config = cash_flow_config.get('output', {})
        output_file = output_config.get('file', 'futu_cash_flow.csv')
        
        processed_data.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"处理完成的数据已成功保存到文件: {output_file}")

    except Exception as e:
        logger.error(f"执行现金流下载任务失败: {e}", exc_info=True)

if __name__ == "__main__":
    main()