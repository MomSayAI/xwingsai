# File: tools/amplitude_statistics.py
# Purpose: 统计历史 prev_amplitude_pct 的分布情况
# Usage: python tools/amplitude_statistics.py

import pandas as pd
import numpy as np
from pathlib import Path
import glob

def analyze_amplitude_distribution(csv_path: Path):
    """分析单个CSV文件的振幅分布"""
    try:
        # 读取CSV文件
        df = pd.read_csv(csv_path)
        
        # 提取 prev_amplitude_pct 列，排除空值
        amplitudes = df['prev_amplitude_pct'].dropna()
        
        if len(amplitudes) == 0:
            print(f"{csv_path.name}: 没有振幅数据")
            return None
        
        # 定义区间
        bins = [0, 0.1, 0.3, 0.5, 1, float('inf')]
        labels = ['<0.1%', '0.1-0.3%', '0.3-0.5%', '0.5-1%', '>1%']
        
        # 统计分布
        distribution = pd.cut(amplitudes, bins=bins, labels=labels, right=False)
        counts = distribution.value_counts().sort_index()
        percentages = (counts / len(amplitudes) * 100).round(1)
        
        # 基本统计
        stats = {
            '文件名': csv_path.name,
            '总样本数': len(amplitudes),
            '最小值': f"{amplitudes.min():.4f}%",
            '最大值': f"{amplitudes.max():.4f}%",
            '平均值': f"{amplitudes.mean():.4f}%",
            '中位数': f"{amplitudes.median():.4f}%",
            '标准差': f"{amplitudes.std():.4f}%"
        }
        
        return {
            'stats': stats,
            'distribution': counts,
            'percentages': percentages
        }
        
    except Exception as e:
        print(f"处理 {csv_path.name} 时出错: {str(e)}")
        return None

def print_results(results, symbol_filter=None):
    """打印统计结果"""
    for file_path, result in results.items():
        if result is None:
            continue
            
        # 如果指定了交易对过滤
        if symbol_filter and symbol_filter not in file_path.name:
            continue
            
        print("\n" + "="*80)
        print(f"文件: {result['stats']['文件名']}")
        print("="*80)
        
        # 打印基本统计
        print("\n📊 基本统计:")
        for key, value in result['stats'].items():
            if key != '文件名':
                print(f"  {key}: {value}")
        
        # 打印分布
        print("\n📈 分布情况:")
        print(f"{'区间':<12} {'次数':<8} {'百分比':<8}")
        print("-"*30)
        
        for i, label in enumerate(['<0.1%', '0.1-0.3%', '0.3-0.5%', '0.5-1%', '>1%']):
            count = result['distribution'].get(label, 0)
            pct = result['percentages'].get(label, 0)
            if count > 0:
                bar = '█' * int(pct / 5)  # 每5%一个█
                print(f"{label:<12} {count:<8} {pct:<6.1f}% {bar}")
        
        print()

def export_to_csv(results, output_path: Path):
    """导出统计结果到CSV"""
    rows = []
    for file_path, result in results.items():
        if result is None:
            continue
            
        row = {
            '文件': result['stats']['文件名'],
            '总样本数': result['stats']['总样本数'],
            '最小值': result['stats']['最小值'],
            '最大值': result['stats']['最大值'],
            '平均值': result['stats']['平均值'],
            '中位数': result['stats']['中位数'],
            '标准差': result['stats']['标准差'],
            '<0.1%_次数': result['distribution'].get('<0.1%', 0),
            '<0.1%_百分比': f"{result['percentages'].get('<0.1%', 0):.1f}%",
            '0.1-0.3%_次数': result['distribution'].get('0.1-0.3%', 0),
            '0.1-0.3%_百分比': f"{result['percentages'].get('0.1-0.3%', 0):.1f}%",
            '0.3-0.5%_次数': result['distribution'].get('0.3-0.5%', 0),
            '0.3-0.5%_百分比': f"{result['percentages'].get('0.3-0.5%', 0):.1f}%",
            '0.5-1%_次数': result['distribution'].get('0.5-1%', 0),
            '0.5-1%_百分比': f"{result['percentages'].get('0.5-1%', 0):.1f}%",
            '>1%_次数': result['distribution'].get('>1%', 0),
            '>1%_百分比': f"{result['percentages'].get('>1%', 0):.1f}%"
        }
        rows.append(row)
    
    if rows:
        df_summary = pd.DataFrame(rows)
        df_summary.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n📁 统计结果已导出到: {output_path}")

def main():
    # 设置路径
    project_root = Path(__file__).resolve().parents[1]
    signals_dir = project_root / 'signals'
    
    if not signals_dir.exists():
        print(f"错误: 找不到 signals 目录: {signals_dir}")
        return
    
    # 获取所有信号CSV文件
    csv_files = list(signals_dir.glob('*_signals.csv'))
    
    if not csv_files:
        print("没有找到信号文件")
        return
    
    print(f"找到 {len(csv_files)} 个信号文件")
    
    # 分析所有文件
    results = {}
    for csv_file in csv_files:
        result = analyze_amplitude_distribution(csv_file)
        if result:
            results[csv_file] = result
    
    # 打印所有结果
    for file_path in results:
        print_results({file_path: results[file_path]})
    
    # 导出汇总到CSV
    output_path = signals_dir / 'amplitude_statistics_summary.csv'
    export_to_csv(results, output_path)
    
    # 可选：按交易对过滤查看
    # print_results(results, symbol_filter='BTC')
    # print_results(results, symbol_filter='ETH')

if __name__ == '__main__':
    main()