import pandas as pd
from datetime import datetime

def calculate_nasdaq_stats(data, start_date, end_date, output_file):
    """
    计算指定时间段内纳斯达克指数周一到周五的统计信息
    
    参数:
        data: 包含历史数据的DataFrame
        start_date: 开始日期 (格式: 'MM/DD/YYYY')
        end_date: 结束日期 (格式: 'MM/DD/YYYY')
        output_file: 输出Excel文件名
        
    返回:
        无，结果保存到Excel文件
    """
    # 转换日期格式
    data['date'] = pd.to_datetime(data['date'], format='%m/%d/%Y')
    data = data.sort_values('date', ascending=True) 
    start_date = datetime.strptime(start_date, '%m_%d_%Y')
    end_date = datetime.strptime(end_date, '%m_%d_%Y')
    
    # 筛选日期范围
    mask = (data['date'] >= start_date) & (data['date'] <= end_date)
    filtered_data = data.loc[mask].copy()
    
    # 计算每日涨跌幅 (基于收盘价)
    filtered_data['daily_return'] = filtered_data['Closing Price'].pct_change() * 100
    
    # 添加星期几信息
    filtered_data['weekday'] = filtered_data['date'].dt.day_name()
    
    # 按星期几分组统计
    weekday_stats = filtered_data.groupby('weekday')['daily_return'].agg(
        ['mean', 'count', lambda x: (x > 0).sum(), lambda x: (x < 0).sum()]
    )
    
    # 重命名列
    weekday_stats.columns = ['average_return', 'total_days', 'up_days', 'down_days']
    
    # 计算上涨下跌占比
    weekday_stats['up_ratio'] = weekday_stats['up_days'] / weekday_stats['total_days'] * 100
    weekday_stats['down_ratio'] = weekday_stats['down_days'] / weekday_stats['total_days'] * 100
    
    # 按周一到周五顺序排序
    weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    weekday_stats = weekday_stats.reindex(weekday_order)
    
    # 保存到Excel
    weekday_stats.to_excel(output_file)
    print(f"统计结果已保存到 {output_file}")

if __name__ == "__main__":
    orig_data_path = "C:\\Users\\199632517\\life\\投资\\data\\HistoricalData_历史.csv"
    df = pd.read_csv(orig_data_path)
    start_date = '08_06_2015'
    end_date = '08_05_2025'
    output_data_path = f"C:\\Users\\199632517\\life\\投资\\output\\nasdaq_weekday_stats_{start_date}_{end_date}.xlsx"
    calculate_nasdaq_stats(df, start_date, end_date, output_data_path)