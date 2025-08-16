import pandas as pd

def calculate_portfolio_returns(start_year, end_year, initial_nasdaq, initial_gold, data_file):
    # 读取数据
    df = pd.read_excel(data_file)
    
    # 筛选指定年份范围的数据
    df = df[(df['年份（1月1日）'] >= start_year) & (df['年份（1月1日）'] <= end_year)].copy()
    df.sort_values('年份（1月1日）', inplace=True)
    
    if len(df) < 2:
        raise ValueError("至少需要两年的数据来计算收益率")
    
    # 计算每年的收益率
    df['纳指收益率'] = df['纳指'].pct_change()
    df['黄金收益率'] = df['黄金'].pct_change()
    
    # 初始化投资价值
    nasdaq_value = initial_nasdaq
    gold_value = initial_gold
    total_value = initial_nasdaq + initial_gold
    
    # 存储结果的二维列表
    result = []
    
    # 添加起始年数据
    result.append([
        f"{start_year}-{start_year}",
        nasdaq_value,
        gold_value,
        total_value,
        0.0,    # 纳指收益率
        0.0,    # 黄金收益率
        0.0     # 组合总收益率
    ])
    
    # 计算每年的投资价值和收益率
    for i in range(1, len(df)):
        current_year = df.iloc[i]['年份（1月1日）']
        nasdaq_return = df.iloc[i]['纳指收益率']
        gold_return = df.iloc[i]['黄金收益率']
        
        # 更新投资价值
        nasdaq_value *= (1 + nasdaq_return)
        gold_value *= (1 + gold_return)
        new_total_value = nasdaq_value + gold_value
        
        # 计算组合总收益率
        total_return = (new_total_value - total_value) / total_value
        
        # 添加到结果列表
        result.append([
            f"{start_year}-{current_year}",
            round(nasdaq_value, 2),
            round(gold_value, 2),
            round(new_total_value, 2),
            round(nasdaq_return, 4),
            round(gold_return, 4),
            round(total_return, 4)
        ])
        
        total_value = new_total_value
    
    # 计算年化收益率（CAGR）
    years = end_year - start_year
    final_total = result[-1][3]
    initial_total = initial_nasdaq + initial_gold
    total_cagr = (final_total / initial_total) ** (1/years) - 1
    
    # 添加年化收益率信息
    cagr_info = [
        "年化收益率(CAGR)",
        "-",
        "-",
        "-",
        "-",
        "-",
        round(total_cagr, 4)
    ]
    
    return result, cagr_info

# 示例使用
if __name__ == "__main__":
    # 参数设置
    start_year = 2011
    end_year = 2025
    initial_nasdaq = 10000  # 初始纳指投资金额
    initial_gold = 10000     # 初始黄金投资金额
    data_file = 'C:\\Users\\199632517\\life\\投资\\data\\纳指黄金年度数据.xlsx'  # 数据文件
    
    # 计算结果
    returns_data, cagr = calculate_portfolio_returns(start_year, end_year, initial_nasdaq, initial_gold, data_file)
    
    # 打印结果
    print("起止年\t\t纳指金额\t黄金金额\t组合总金额\t纳指收益率\t黄金收益率\t组合总收益率")
    for row in returns_data:
        print(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]:.2%}\t{row[5]:.2%}\t{row[6]:.2%}")
    
    # 打印年化收益率
    print(f"\n{cagr[0]}\t{cagr[1]}\t{cagr[2]}\t{cagr[3]}\t{cagr[4]}\t{cagr[5]}\t{cagr[6]:.2%}")
    
    # 写入Excel
    output_df = pd.DataFrame(
        returns_data,
        columns=['起止年', '纳指金额', '黄金金额', '组合总金额', '纳指收益率', '黄金收益率', '组合总收益率']
    )
    
    # 添加年化收益率到最后一行
    cagr_df = pd.DataFrame([cagr], columns=output_df.columns)
    output_df = pd.concat([output_df, cagr_df], ignore_index=True)
    
    output_df.to_excel(f'output\\investment_returns_output_{start_year}_{end_year}.xlsx', index=False)
    print("\n结果已保存到 investment_returns_output.xlsx")