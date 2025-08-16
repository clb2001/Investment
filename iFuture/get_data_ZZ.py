#!/usr/bin/env python
#-*- coding: utf-8 -*-
'''
Created on July 5, 2022
@author: clb
'''
import csv
import os
import re
import urllib.error
from urllib.request import urlopen
# from bs4 import BeautifulSoup
import json
import pandas as pd
import socket
import time
import requests
from bs4 import BeautifulSoup

socket.setdefaulttimeout(20)
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
        self.dumpfold = "D:/internship/history_data/dumpfold"
        # self.dumpfold = "/data/intern_2/data/diskData"
        self.dominantfold = "D:/internship/data/inst_cf/inst_c"
        # self.dominantfold = "/home/intern/data/inst_cf/inst_c"
        self.reference = 'D:/internship/history_data/reference/trade_scale.csv'
        # self.tick = ['AP', 'CF', 'CJ', 'FG', 'MA', 'PF', 'PK', 'RM', 'SA', 'SR', 'TA', 'UR', 'ZC']
        self.tick = ['CJ']
        self.columns = ["TradingDay UpdateTime", "Buy", "Sell", "ClosePrice", "Volume",
                         "Position", "WarehouseReceipt", "NetOpenInterest", "PositionIndex" ,'DepositCapital']

    def run(self):
        for tickname in self.tick:
            print(tickname)
            df = pd.read_csv(self.dominantfold + "/" + tickname + ".csv", header=0, index_col=0,
                             names=['date', 'contract'])
            # 路径不存在需要新建
            filename = self.dumpfold + '/' + tickname + '.csv'
            file = pd.DataFrame([], columns=self.columns)
            file.to_csv(filename, index=False)
            self.inst(df, filename, tickname)

    # 读取主力合约文件夹的数据
    def inst(self, df, filename, tickname):
        j = 0
        data = pd.read_csv(filename, header=0, names=self.columns,engine='python')
        for date in df.index.values:
            if date < 20220530 or date > 20220601: continue
            print('LastestTime:',date)
            dominant_contract = df.loc[date, 'contract']
            if dominant_contract == '0' or 0: continue
            data.loc[j, "TradingDay UpdateTime"] = date
            # self.search_1(date, dominant_contract, data, j)
            # self.search_2(date, dominant_contract, data, j)
            self.search_3(date, data, j, tickname)
            # self.search_4(date, data, j, dominant_contract, tickname)
            if pd.isnull(data.loc[j, 'Sell']) or pd.isnull(data.loc[j, 'Buy']):
                data.loc[j, 'NetOpenInterest'] = ''
                data.loc[j, 'PositionIndex'] = ''
            else:
                data.loc[j, 'NetOpenInterest'] = int(data.loc[j, 'Buy']) - int(data.loc[j, 'Sell'])
                data.loc[j, 'PositionIndex'] = (int(data.loc[j, 'Buy']) - int(data.loc[j, 'Sell']))\
                                               /(int(data.loc[j, 'Sell']) + int(data.loc[j, 'Buy']))
            j += 1
        data.to_csv(filename, index=False, mode='a', header=False)

    # 查找收盘价、成交量、持仓量
    def search_1(self, date, dominant_contract, data, j):
        try:
            date = str(date)
            # 获得数据存储的url='http://www.czce.com.cn/cn/DFSStaticFiles/Future/2016/20160104/FutureDataDaily.htm'
            'http://www.czce.com.cn/cn/DFSStaticFiles/Future/2016/20160104/FutureDataDaily.htm'
            html_1 = requests.get('http://www.czce.com.cn/cn/DFSStaticFiles/Future/'+ date[0:4] + '/' + date + '/FutureDataDaily.htm')
            html_1.encoding = 'utf-8'
            # html = urlopen("http://www.shfe.com.cn/data/instrument/ContractDailyTradeArgument20180608.dat")
            bsObj = BeautifulSoup(html_1.text, "html.parser")# 获取网页源代码
            tr = bsObj.find('table').find_all('tr')  # .find定位到所需数据位置  .find_all查找所有的tr（表格）
            # 去除标签栏
            for k in tr[1:]:  # tr2[1:]遍历第1行到最后一行，表头为第0行
                td = k.find_all('td')  # td表格
                DOMINANT_CONTRACT = td[0].get_text().strip()  # 遍历品种月份
                if DOMINANT_CONTRACT != dominant_contract: continue
                data.loc[j, 'ClosePrice'] = int(float(td[5].get_text().strip().replace(",", "")))
                data.loc[j, 'Volume'] = int(float(td[9].get_text().strip().replace(",", "")))
                data.loc[j, 'Position'] = int(float(td[10].get_text().strip().replace(",", "")))
            html_1.close()
        except urllib.error.URLError as e:
            print(e.reason)
        except socket.timeout as e:
            print(type(e))
        time.sleep(0.1)

     # 查找持空单量、持多单量
    def search_2(self, date, dominant_contract, data, j):
        try:
            date = str(date)
            # 获得数据存储的url='http://www.czce.com.cn/cn/DFSStaticFiles/Future/2016/20160104/FutureDataDaily.htm'
            'http://www.czce.com.cn/cn/DFSStaticFiles/Future/2022/20220705/FutureDataHolding.htm'
            html_2 = requests.get('http://www.czce.com.cn/cn/DFSStaticFiles/Future/'+ date[0:4] + '/' + date + '/FutureDataHolding.htm')
            html_2.encoding = 'utf-8'
            # html = urlopen("http://www.shfe.com.cn/data/instrument/ContractDailyTradeArgument20180608.dat")
            bsObj = BeautifulSoup(html_2.text, "html.parser")# 获取网页源代码
            tr = bsObj.find('table').find_all('tr')  # .find定位到所需数据位置  .find_all查找所有的tr（表格）
            for i in range(len(tr)):  # tr2[1:]遍历第1行到最后一行，表头为第0行
                if dominant_contract in tr[i].find('td').get_text().strip():
                    k = 1
                    while True:
                        td = tr[i+k].find_all('td')
                        LIST = td[0].get_text().strip()
                        if LIST == '合计':
                            data.loc[j, 'Buy'] = td[5].get_text().strip().replace(",", "")
                            data.loc[j, 'Sell'] = td[8].get_text().strip().replace(",", "")
                            break
                        else: k += 1
                else: continue
            html_2.close()
        except urllib.error.URLError as e:
            print(e.reason)
        except socket.timeout as e:
            print(type(e))
        time.sleep(1)

    # 查找仓单量
    def search_3(self, date, data, j, tickname):
        try:
            'http://www.czce.com.cn/cn/DFSStaticFiles/Future/2022/20220706/FutureDataWhsheet.htm'
            date = str(date)
            html_3 = requests.get('http://www.czce.com.cn/cn/DFSStaticFiles/Future/' + date[0:4] + '/' + date + '/FutureDataWhsheet.htm')
            # html_3 = requests.get('http://www.czce.com.cn/cn/DFSStaticFiles/Future/2016/20160104/FutureDataWhsheet.htm')
            html_3.encoding = 'utf-8'
            bsObj = BeautifulSoup(html_3.text, "html.parser") # 获取网页源代码
            table = bsObj.find_all('table')  # .find定位到所需数据位置  .find_all查找所有的tr（表格）
            if '本年第' in table[0].text:
                start = 1
            else:
                start = 0
            for i in range(start, len(table)):  # tr2[1:]遍历第1行到最后一行，表头为第0行
                tr = table[i].find_all('tr')
                td_0 = tr[0].find('td')
                td_1 = tr[1].find_all('td')
                if tickname in td_0.get_text().strip():
                    k = 1
                    while True:
                        td = tr[k].find_all('td')
                        LIST = td[0].get_text().strip()
                        if LIST == '总计':
                            if td[2].find('table') != None:
                                sub_tr = td[2].find('table').find_all('tr')
                                data.loc[j, 'WarehouseReceipt'] = int(sub_tr[1].find('td').get_text().strip().replace(",", ""))
                                print('CJ:',data.loc[j, 'WarehouseReceipt'])
                            else:
                                s = 0
                                while True:
                                    if td_1[s].get_text().strip() == '仓单数量':
                                        data.loc[j, 'WarehouseReceipt'] = int(td[s].get_text().strip().replace(",", ""))
                                        print(data.loc[j, 'WarehouseReceipt'])
                                        break
                                    elif td_1[s].get_text().strip() == '仓单数量(完税)' and td_1[s+1].get_text().strip() == '仓单数量(保税)':
                                        data.loc[j, 'WarehouseReceipt'] = int(td[s].get_text().strip().replace(",", "")) \
                                                                          + int(td[s + 1].get_text().strip().replace(",", ""))
                                        print('c')
                                        break
                                    else:
                                        s += 1
                            break
                        else:
                            k += 1
                else:
                    continue
            html_3.close()
        except urllib.error.URLError as e:
            print(e.reason)
        except socket.timeout as e:
            print(type(e))
        time.sleep(0.1)

    # 计算沉淀资金
    def search_4(self, date, data, j, dominant_contract, tickname):
        # 沉淀资金=收盘价 * 单位（取自对应的数据项） * 交易保证金 / 100  * 空盘量 * 2
        'http://www.czce.com.cn/cn/DFSStaticFiles/Future/2022/20220706/FutureDataClearParams.htm'
        try:
            date = str(date)
            html_4 = requests.get("http://www.czce.com.cn/cn/DFSStaticFiles/Future/" + date[0:4] + '/' + date + '/FutureDataClearParams.htm')
            html_4.encoding ='utf-8'
            bsObj = BeautifulSoup(html_4.text, "html.parser")  # 获取网页源代码
            table = bsObj.find('table')  # .find定位到所需数据位置  .find_all查找所有的tr（表格）
            tr = table.find_all('tr')
            for i in range(len(tr)):
                td = tr[i].find_all('td')
                if dominant_contract == td[0].get_text().strip():
                    a = int(float(td[4].get_text().strip()))
                    scale_reference = pd.read_csv(self.reference, header=0, index_col=None, engine='python',names=['id','scale'])
                    print(scale_reference)
                    scale = scale_reference.iloc[scale_reference[scale_reference['id'] == tickname].index,1]
                    data.loc[j, 'DepositCapital'] = 2 * a * int(data.loc[j, 'ClosePrice']) * int(data.loc[j, 'Position']) * int(scale) / (10 ** 10)
                    print(data.loc[j, 'DepositCapital'])
                else: continue
            html_4.close()
        except urllib.error.URLError as e:
            print(e.reason)
        except socket.timeout as e:
            print(type(e))
        time.sleep(0.1)

if __name__ == "__main__":
    job = get_data()
    job.run()

