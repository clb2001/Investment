# coding = 'utf-8'
'''
Created on July 5, 2022
@author: clb
'''
import csv
import os
import pandas as pd
from chardet import detect

class get_data(object):
    def __init__(self):
        # 以上海期货交易所为例
        # 需要获得的数据内容及位置：
        # 流入资金、流出资金——
        # 持空单量、持多单量——交易数据-日统计数据-日交易排名-持买单量（多单）、持卖单量（空单）
        # 收盘价——交易数据-日统计数据-日交易快讯-商品名称:螺纹钢-（主力合约）-收盘价
        # 成交量——交易数据-日统计数据-日交易快讯-商品名称:螺纹钢-（主力合约）-成交手
        # 持仓量——交易数据-日统计数据-日交易快讯-商品名称:螺纹钢-（主力合约）-持仓手
        # 仓单量——交易数据-日统计数据-仓单日报-螺纹钢（仓库+厂库）-正则匹配-总计
        # 净持仓量=持多单量-持空单量
        # 持仓指标=（多单数量-空单数量）/（多单数量+空单数量）*100%
        self.dumpfold = "D:/internship/history_data/dumpfold_DL"
        self.dominantfold = "D:/internship/data/inst_cf/inst_c"
        self.basefold = 'D:/internship/history_data/basefold_DL'
        # self.tick = [ 'c', 'cs', 'eb', 'eg', 'i', 'j', 'jm', 'l',
        #              'lh', 'm', 'p', 'pg', 'pp', 'v', 'y']
        self.tick = ['eg']
        self.columns = ["TradingDay UpdateTime", "ClosePrice", "Volume", "Position"]

    def run(self):
        for tickname in self.tick:
            print(tickname)
            df = pd.read_csv(self.dominantfold + "/" + tickname + ".csv", header=0, index_col=0,
                             names=['date', 'contract'])
            # 路径不存在需要新建
            filename = self.dumpfold + '/' + tickname + '.xlsx'
            file = pd.DataFrame([], columns=self.columns)
            file.to_excel(filename, index=False)
            # self.encoding()
            self.inst(df, filename, tickname)

    # # 统一文件编码
    # def encoding(self):
    #     L = []
    #     for root, dirs, files in os.walk(self.basefold):
    #         # 获得所有csv文件
    #         for file in files:
    #             if os.path.splitext(file)[1] == '.csv':
    #                 L.append(os.path.join(root, file))
    #     if len(L) > 0:
    #         for path in L:
    #             print(path)
    #             # 修改编码格式
    #             with open(path, 'rb') as fp:
    #                 content = fp.read()
    #                 encoding = detect(content)['encoding']
    #                 print(encoding)
    #                 content = content.decode(encoding).encode('utf8')
    #                 fp.seek(0)
    #                 fp.write(content)

    # 读取主力合约文件夹的数据
    def inst(self, df, filename, tickname):
        j = 0
        data = pd.read_excel(filename, header=0, names=self.columns, engine='openpyxl')
        for date in df.index.values:
            if date < 20161231 or date > 20211231: continue
            print('LastestTime:', date)
            dominant_contract = df.loc[date, 'contract']
            if dominant_contract == '0' or 0: continue
            data.loc[j, "TradingDay UpdateTime"] = date
            date = str(date)
            year = date[0:4]
            combine = tickname + year
            reference_fold = self.basefold + "/" + combine + ".xlsx"
            if os.path.exists(reference_fold):
                self.search_1(date, dominant_contract, data, j, tickname, reference_fold)
            else:
                continue
            j += 1
        data.to_excel(filename, index=False, header=False)

    # 查找收盘价、成交量、持仓量
    def search_1(self, date, dominant_contract, data, j, tickname, reference_fold):
        df = pd.read_excel(reference_fold, header=0)
        for i in range(len(df)):
            if df.loc[i,'合约'] == dominant_contract and str(df.loc[i,'日期']) == date:
                data.loc[j,'ClosePrice'] = df.loc[i,'收盘价']
                data.loc[j, 'Volume'] = df.loc[i, '成交量']
                data.loc[j, 'Position'] = df.loc[i, '持仓量']
                print(data.loc[j, 'Position'])
            else:
                continue

if __name__ == "__main__":
    job = get_data()
    job.run()
    # os.chdir(r'D:/internship/history_data/basefold_DL')
    #
    # # 列出当前目录下所有的文件
    # files = os.listdir('./')
    # print('files', files)
    #
    # for fileName in files:
    #     portion = os.path.splitext(fileName)
    #     # 如果后缀是.dat
    #     if portion[1] == ".csv":
    #         # 把原文件后缀名改为 txt
    #         newName = portion[0] + ".xlsx"
    #         os.rename(fileName, newName)




