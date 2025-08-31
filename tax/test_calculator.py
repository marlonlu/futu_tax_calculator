import os
import subprocess
import pandas as pd
import sys
import re
from stock_option_tax_calculator import calculate_tax

# 确保可以从主脚本导入 classify_asset 函数
try:
    from stock_option_tax_calculator import classify_asset
except ImportError:
    print("错误：无法从 stock_option_tax_calculator.py 导入 classify_asset。")
    print("请确保 test_calculator.py 和 stock_option_tax_calculator.py 在同一目录下。")
    sys.exit(1)

def run_test_case(test_dir, input_filename):
    """
    运行单个测试用例。
    """
    input_path = os.path.join(test_dir, input_filename)
    output_path = os.path.join(test_dir)

    # 运行计算器脚本
    calculate_tax(input_path, output_path)

    # 查找预期的输出文件以确定年份
    expected_filename_pattern = re.compile(r'test_data_(\d{4})\.csv')
    expected_file = None
    year = None
    for f in os.listdir(test_dir):
        match = expected_filename_pattern.match(f)
        if match:
            expected_file = os.path.join(test_dir, f)
            year = match.group(1)
            generated_path = os.path.join(test_dir, f'{year}_report.csv')
            success, message = compare_results(generated_path, expected_file)
            if not success:
                return success, message

    if not expected_file:
        return False, f"警告：在 {test_dir} 中未找到预期的输出文件 (例如, test_data_2023.csv)。跳过此目录。"

    # 比较结果
    return True, ""


def safe_format_datetime(date_series):
    """
    安全地将日期系列转换为 '%Y-%m-%d %H:%M:%S' 格式的字符串。
    如果日期格式非法，则返回 'unknown'。
    """
    # 使用 errors='coerce' 将无效日期转换为 NaT
    datetimes = pd.to_datetime(date_series, errors='coerce')

    # 将日期格式化为字符串，NaT 会变成 NaN
    formatted_strings = datetimes.dt.strftime('%Y-%m-%d')

    # 将 NaN 值替换为 'unknown'
    return formatted_strings.fillna('unknown')


def compare_results(generated_path, expected_path):
    """
    比较生成的CSV和预期的CSV文件。
    """
    try:
        generated_df = pd.read_csv(generated_path)
        expected_df = pd.read_csv(expected_path)
    except FileNotFoundError as e:
        return False, f"比较失败: 文件未找到 - {e.filename}"

    # 创建用于比较的唯一键
    generated_df['key'] = generated_df['股票代码'].astype(str) + '_' + safe_format_datetime(generated_df['时间'])
    expected_df['key'] = expected_df['股票代码'].astype(str) + '_' + safe_format_datetime(expected_df['时间'])

    generated_map = generated_df.set_index('key').to_dict('index')
    expected_map = expected_df.set_index('key').to_dict('index')

    # 生成的结果包含有年度核算，数量不能对的上
    # if len(generated_map) != len(expected_map):
    #     return False, f"比较失败: 记录数量不匹配。预期: {len(expected_map)}, 实际: {len(generated_map)}"

    for key, expected_row in expected_map.items():
        if "年度" in key:
            continue

        if key not in generated_map:
            return False, f"比较失败: 预期记录未在生成结果中找到，键: {key}"
        
        generated_row = generated_map[key]
        asset_type = classify_asset(expected_row['股票代码'])

        # 考虑到四位小数点之间计算，容许一定的容差
        price_tolerance = 0.1 if asset_type == 'Option' else 1.0
        profit_tolerance = 1.0

        # 比较字段
        fields_to_compare = ['卖出价格', '成本价', '利润']
        for field in fields_to_compare:
            expected_val = float(expected_row[field])
            generated_val = float(generated_row[field])
            tolerance = profit_tolerance if field == '利润' else price_tolerance
            
            if abs(expected_val - generated_val) > tolerance:
                return False, f"比较失败: 键 '{key}' 的字段 '{field}' 不匹配。\n预期: {expected_val}, 实际: {generated_val}, 容差: {tolerance}"

        if int(expected_row['数量']) != int(generated_row['数量']):
             return False, f"比较失败: 键 '{key}' 的字段 '数量' 不匹配。\n预期: {expected_row['数量']}, 实际: {generated_row['数量']}"


    print(f"✅ 验证通过: {os.path.basename(generated_path)} 与 {os.path.basename(expected_path)} 一致。")
    return True, ""


def main():
    """
    主测试函数。
    """
    test_data_dir = os.path.join(os.path.dirname(__file__), '..', 'test_data', '')
    # test_data_dir = '../test_data'
    all_tests_passed = True
    
    if not os.path.isdir(test_data_dir):
        print(f"错误: 测试数据目录 '{test_data_dir}' 未找到。")
        sys.exit(1)

    # 递归遍历测试目录
    for root, dirs, files in os.walk(test_data_dir):
        # 我们只在包含 test_data.csv 的目录中运行测试
        key_csv = 'test_data.csv'
        if key_csv in files:
            print(f"开始测试数据目录 '{root}'")
            success, message = run_test_case(root, key_csv)
            if not success:
                all_tests_passed = False
                print(f"❌ {root} {message}\n")
                return # 中断用例

    print("\n" + "="*30)
    if all_tests_passed:
        print("🎉 所有测试用例均已通过！")
    else:
        print("🔥 部分测试用例失败。")
    print("="*30)

if __name__ == '__main__':
    main()
