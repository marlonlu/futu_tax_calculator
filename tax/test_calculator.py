# test_calculator.py

import os
import re
import sys
from typing import List, Tuple, Dict

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

# --- 核心导入 ---
# 从主程序中导入需要测试的函数。
try:
    from stock_option_tax_calculator import _process_all_transactions, classify_asset, preprocess_data
except ImportError:
    print("错误：无法从 stock_option_tax_calculator.py 导入所需函数。")
    print("请确保该文件存在，并且包含 _process_all_transactions, classify_asset, 和 preprocess_data 函数。")
    sys.exit(1)

# ==============================================================================
# 数据比较层 (Data Comparison Tier)
# ==============================================================================

# 定义容差规则为模块级常量，便于维护
FLOAT_COLS_WITH_TOLERANCE = {
    '卖出价格': {'Stock': 1.0, 'Option': 0.1},
    '成本价': {'Stock': 1.0, 'Option': 0.1},
    '利润': {'Stock': 1.0, 'Option': 1.0}
}
COLS_TO_COMPARE = ['股票代码', '卖出价格', '成本价', '数量', '利润', '时间', '结算币种']
EXACT_COLS = ['股票代码', '时间', '数量', '结算币种']


def _prepare_and_align_dataframes(
        gen_df: pd.DataFrame, exp_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """职责: 预处理DF，统一类型并排序对齐，以便比较。"""
    gen = gen_df[COLS_TO_COMPARE].copy()
    exp = exp_df[COLS_TO_COMPARE].copy()

    if len(gen) != len(exp):
        raise AssertionError(f"行数不匹配: 生成了 {len(gen)} 行, 预期 {len(exp)} 行。")

    for df in [gen, exp]:
        df['时间'] = pd.to_datetime(df['时间']).dt.strftime('%Y-%m-%d')
        for col in FLOAT_COLS_WITH_TOLERANCE:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        df['数量'] = pd.to_numeric(df['数量'], errors='coerce').astype(int)

    sort_keys = ['股票代码', '时间', '数量']
    return (
        gen.sort_values(by=sort_keys).reset_index(drop=True),
        exp.sort_values(by=sort_keys).reset_index(drop=True)
    )


def _assert_exact_columns_match(
        gen: pd.DataFrame, exp: pd.DataFrame, test_case_name: str
) -> None:
    """职责: 断言需要精确匹配的列是否一致。"""
    try:
        assert_frame_equal(gen[EXACT_COLS], exp[EXACT_COLS], check_dtype=False)
    except AssertionError as e:
        raise AssertionError(f"[{test_case_name}] 精确列不匹配 (如代码, 时间, 数量等):\n{e}")


def _find_tolerance_failures(gen: pd.DataFrame, exp: pd.DataFrame) -> List[Dict]:
    """职责: 找出所有超出自定义容差的数值，并收集失败详情。"""
    failures = []
    asset_types = gen['股票代码'].apply(classify_asset)
    is_option_mask = (asset_types == 'Option')

    for col, tolerances in FLOAT_COLS_WITH_TOLERANCE.items():
        diff = (gen[col] - exp[col]).abs()
        tolerance_values = np.where(is_option_mask, tolerances['Option'], tolerances['Stock'])
        failed_mask = diff > tolerance_values

        if failed_mask.any():
            for idx in gen.index[failed_mask]:
                failures.append({
                    '股票代码': gen.loc[idx, '股票代码'], '比较列': col,
                    '生成值': gen.loc[idx, col], '预期值': exp.loc[idx, col],
                    '差值': diff.loc[idx], '容差': tolerance_values[idx]
                })
    return failures


def _report_and_raise_failures(failures: List[Dict], test_case_name: str) -> None:
    """职责: 如果存在失败，则格式化错误报告并抛出异常。"""
    if not failures:
        return

    error_df = pd.DataFrame(failures)
    error_message = f"[{test_case_name}] 比较失败！数值超出自定义容差范围。\n失败详情:\n"
    with pd.option_context('display.max_rows', None, 'display.width', 200):
        error_message += error_df.to_string(index=False)
    raise AssertionError(error_message)


def compare_dataframes(generated_df: pd.DataFrame, expected_df: pd.DataFrame, test_case_name: str) -> None:
    """
    协调器：使用自定义容差比较两个DataFrame。
    遵循单一职责原则，将复杂逻辑委托给辅助函数。
    """
    try:
        gen, exp = _prepare_and_align_dataframes(generated_df, expected_df)

        _assert_exact_columns_match(gen, exp, test_case_name)

        failures = _find_tolerance_failures(gen, exp)

        _report_and_raise_failures(failures, test_case_name)

        print(f"    ✅ 验证通过: {test_case_name} 的数据一致。")

    except KeyError as e:
        raise AssertionError(f"[{test_case_name}] 比较失败：字段缺失 - {e}")


# ==============================================================================
# 测试执行层 (Test Execution Tier)
# ==============================================================================

def run_test_case(test_dir: str) -> None:
    """
    在内存中执行单个测试用例的完整流程：加载、计算、比较。
    如果测试失败，会抛出 AssertionError。
    """
    test_case_name = os.path.basename(test_dir)
    print(f"--- 开始测试: {test_case_name} ---")

    input_path = os.path.join(test_dir, 'test_data.csv')
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"[{test_case_name}] 失败：找不到输入文件 test_data.csv。")

    # 1. 加载和预处理输入数据
    transactions_df = preprocess_data(input_path)
    transactions_df['资产类型'] = transactions_df['股票代码'].apply(classify_asset)

    if 'invalid' in test_case_name.lower():
        print("    检测到无效数据用例，预期程序会抛出异常...")
        with pytest.raises((ValueError, KeyError, ZeroDivisionError)) as excinfo:
            _process_all_transactions(transactions_df)
        print(f"    ✅ 成功捕获预期异常: {excinfo.type.__name__}")
        return

    # 2. 在内存中执行核心计算逻辑
    generated_report_df = _process_all_transactions(transactions_df)

    # 3. 查找所有预期的输出文件并进行比较
    expected_files = [f for f in os.listdir(test_dir) if f.startswith('test_data_') and f.endswith('.csv')]

    if not expected_files:
        if not generated_report_df.empty:
            raise AssertionError(f"[{test_case_name}] 失败：程序生成了数据，但没有找到任何预期的输出文件。")
        else:
            print("    ✅ 验证通过: 程序和预期均未生成任何数据。")
            return

    for expected_filename in expected_files:
        match = re.search(r'(\d{4})', expected_filename)
        if not match:
            continue

        year = int(match.group(1))
        print(f"  校验年份: {year}...")

        expected_df = pd.read_csv(os.path.join(test_dir, expected_filename))
        generated_df_year = generated_report_df[
            pd.to_datetime(generated_report_df['时间']).dt.year == year
            ].copy()

        compare_dataframes(generated_df_year, expected_df, f"{test_case_name}/{year}")


# ==============================================================================
# 测试发现与管理层 (Test Runner Tier)
# ==============================================================================

def find_test_cases(root_dir: str) -> List[str]:
    """
    发现所有有效的测试用例目录，即包含 'test_data.csv' 的目录。
    """
    test_case_dirs = []
    if not os.path.isdir(root_dir):
        print(f"错误: 测试数据目录 '{root_dir}' 未找到。", file=sys.stderr)
        return []

    for dirname in sorted(os.listdir(root_dir)):
        if dirname.startswith('.') or dirname.startswith('_'):
            continue

        potential_test_dir = os.path.join(root_dir, dirname)
        if os.path.isdir(potential_test_dir) and 'test_data.csv' in os.listdir(potential_test_dir):
            test_case_dirs.append(potential_test_dir)

    return test_case_dirs


def main():
    """
    主测试运行器：发现并执行所有测试用例，统一报告结果。
    """
    current_dir = os.path.dirname(__file__)
    # 路径通常相对于项目根目录，这里假设 `test_data` 目录与 `src` 目录同级
    test_data_dir = os.path.abspath(os.path.join(current_dir, '..', 'test_data'))

    test_cases = find_test_cases(test_data_dir)

    if not test_cases:
        print(f"在目录 '{test_data_dir}' 中未找到任何有效的测试用例。请检查路径和目录结构。")
        sys.exit(1)

    failures = []
    for test_dir in test_cases:
        try:
            run_test_case(test_dir)
        except (AssertionError, FileNotFoundError, Exception) as e:
            test_case_name = os.path.basename(test_dir)
            print(f"❌ 测试失败: {test_case_name}\n   原因: {e}\n")
            failures.append(test_case_name)

    print("\n" + "=" * 50)
    print("           测试结果总结")
    print("=" * 50)

    if not failures:
        print(f"🎉 全部 {len(test_cases)} 个测试用例均已通过！")
    else:
        print(f"🔥 测试完成，共 {len(test_cases)} 个用例，其中 {len(failures)} 个失败：")
        for i, f in enumerate(failures, 1):
            print(f"  {i}. {f}")
        sys.exit(1)


if __name__ == '__main__':
    main()
