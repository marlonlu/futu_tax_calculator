import pandas as pd
import argparse
import os
import logging
import yaml
from datetime import datetime

# 导入重构后的模块
from tax.data.preprocessor import preprocess_data, classify_asset
from tax.data.validator import validate_file_exists, clean_and_validate_data
from tax.reports.generator import generate_and_save_reports
from tax.processors.stock_processor import process_stock_transactions
from tax.processors.option_processor import process_option_transactions


def load_config():
    """加载配置文件"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'history_config.yaml')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        # 如果配置文件不存在，返回默认配置
        return {
            'output': {
                'processed_file': 'futu_history.csv'
            },
            'tax_calculation': {
                'rsu_file': 'futu_rsu_history.csv',
                'output_dir': '税务报告'
            }
        }


def setup_logging():
    """配置日志系统"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('tax_calculator.log', encoding='utf-8')
        ]
    )
    return logging.getLogger(__name__)


def merge_rsu_data(transactions_df, input_file, config):
    """
    合并RSU数据的独立函数
    """
    logger = logging.getLogger(__name__)
    
    # 从配置获取RSU文件名
    rsu_filename = config.get('tax_calculation', {}).get('rsu_file', 'futu_rsu_history.csv')
    
    # 构建RSU文件的预期路径
    input_dir = os.path.dirname(input_file)
    rsu_file_path = os.path.join(input_dir, rsu_filename)
    
    # 检查文件是否存在
    if os.path.exists(rsu_file_path):
        logger.info(f"检测到 RSU 历史文件: {rsu_file_path}, 开始合并处理...")
        
        try:
            # 加载并预处理 RSU 数据
            rsu_df = preprocess_data(rsu_file_path)
            if rsu_df is not None and not rsu_df.empty:
                # 合并两个 DataFrame
                logger.info("正在合并主交易数据与 RSU 数据...")
                transactions_df = pd.concat([transactions_df, rsu_df], ignore_index=True)
                
                # 对合并后的数据全局按时间排序
                logger.info("正在按交易时间重新排序所有记录...")
                transactions_df.sort_values(by='交易时间', inplace=True, ignore_index=True)
                logger.info("数据合并与排序完成。")
        except Exception as e:
            logger.error(f"RSU数据合并失败: {e}")
            raise
    else:
        logger.info("未检测到 RSU 历史文件，跳过合并步骤。")
    
    return transactions_df


def generate_sales_records(transactions_df):
    """
    生成交易盈亏记录
    
    Args:
        transactions_df: 预处理后的交易数据DataFrame
        
    Returns:
        list: 包含所有交易盈亏记录的列表
    """
    logger = logging.getLogger(__name__)
    all_sales_records = []
    
    # 调试信息：检查输入数据
    logger.info(f"输入数据行数: {len(transactions_df)}")
    logger.info(f"输入数据列名: {list(transactions_df.columns)}")
    
    if transactions_df.empty:
        logger.warning("输入的交易数据为空")
        return all_sales_records
    
    # 调试信息：检查股票代码分组
    unique_codes = transactions_df['股票代码'].unique()
    logger.info(f"发现 {len(unique_codes)} 个唯一股票代码: {unique_codes[:5]}...")  # 只显示前5个
    
    # 调试信息：检查资产类型分布
    asset_type_counts = transactions_df['资产类型'].value_counts()
    logger.info(f"资产类型分布: {dict(asset_type_counts)}")
    
    # 按"股票代码"分组处理，生成所有交易的盈亏记录
    processed_count = 0
    for code, group_df in transactions_df.groupby('股票代码'):
        asset_type = group_df['资产类型'].iloc[0]
        group_size = len(group_df)
        logger.debug(f"正在处理资产: {code} ({asset_type}), 交易记录数: {group_size}")
        
        try:
            if asset_type == 'Stock':
                stock_results = process_stock_transactions(group_df, code)
                logger.debug(f"股票 {code} 处理结果: {len(stock_results)} 条记录")
                all_sales_records.extend(stock_results)
            elif asset_type == 'Option':
                option_records, _, _, _ = process_option_transactions(group_df, code)
                logger.debug(f"期权 {code} 处理结果: {len(option_records)} 条记录")
                all_sales_records.extend(option_records)
            else:
                logger.warning(f"未知资产类型: {asset_type} (股票代码: {code})")
            
            processed_count += 1
            
        except Exception as e:
            logger.error(f"处理资产 {code} 时发生错误: {e}")
            continue
    
    logger.info(f"处理完成: 共处理 {processed_count} 个资产，生成 {len(all_sales_records)} 条交易盈亏记录")
    
    # 如果没有生成任何记录，输出详细的调试信息
    if not all_sales_records:
        logger.warning("⚠️  没有生成任何交易盈亏记录，可能原因:")
        logger.warning("1. 所有交易都是买入操作，没有卖出操作")
        logger.warning("2. 数据格式不符合处理器要求")
        logger.warning("3. 资产类型分类错误")
        
        # 输出前几行数据样本用于调试
        if not transactions_df.empty:
            logger.info("数据样本 (前3行):")
            for i, row in transactions_df.head(3).iterrows():
                logger.info(f"  行{i}: 股票代码={row.get('股票代码')}, 资产类型={row.get('资产类型')}, "
                          f"交易方向={row.get('交易方向')}, 数量={row.get('数量')}")
    
    return all_sales_records


def calculate_tax(input_file, output_dir):
    """
    重构后的主计算函数
    """
    logger = setup_logging()
    config = load_config()
    
    try:
        # 步骤 1: 验证输入文件
        validate_file_exists(input_file)
        logger.info(f"开始处理文件: {input_file}")
        
        # 步骤 2: 加载和预处理数据
        transactions_df = preprocess_data(input_file)
        logger.info("数据预处理完成")
        
        # 步骤 3: 合并RSU数据（如果存在）
        transactions_df = merge_rsu_data(transactions_df, input_file, config)
        
        # 步骤 4: 区分资产类型
        transactions_df['资产类型'] = transactions_df['股票代码'].apply(classify_asset)
        logger.info("资产类型分类完成")
        
        # 步骤 5: 数据验证
        transactions_df = clean_and_validate_data(transactions_df)
        logger.info("数据验证完成")
        
        # 步骤 6: 生成交易盈亏记录
        sales_records = generate_sales_records(transactions_df)
        
        if not sales_records:
            logger.warning("处理完成，但没有发现任何可报告的卖出交易，因此未生成任何报告。")
            return
        

        # 步骤 7: 生成并保存报告
        reports_were_generated = generate_and_save_reports(sales_records, output_dir)
        
        if reports_were_generated:
            logger.info(f"处理完成，年度报告已保存在目录: {output_dir}")
        else:
            logger.error("报告生成失败")
            
    except Exception as e:
        logger.error(f"处理过程中发生错误: {e}")
        raise


def main():
    """命令行入口函数"""
    # 加载配置
    config = load_config()
    output_config = config.get('output', {})
    tax_config = config.get('tax_calculation', {})
    
    # 从配置获取默认路径
    default_input_filename = output_config.get('processed_file', 'futu_history.csv')
    default_output_dirname = tax_config.get('output_dir', '税务报告')
    
    default_csv_file_path = os.path.join(os.path.dirname(__file__), '..', 'data', default_input_filename)
    default_out_dir_path = os.path.join(os.path.dirname(__file__), '..', default_output_dirname)
    
    parser = argparse.ArgumentParser(description='股票及期权年度报税计算器')
    parser.add_argument('--input', type=str, default=default_csv_file_path, help='输入的CSV文件路径')
    parser.add_argument('--output', type=str, default=default_out_dir_path, help='输出报告的文件夹路径')
    args = parser.parse_args()

    # 确保输出目录存在
    output_dir = args.output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    calculate_tax(args.input, args.output)


if __name__ == '__main__':
    main()