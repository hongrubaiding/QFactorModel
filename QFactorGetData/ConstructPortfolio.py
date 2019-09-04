# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    构建Qfactor中的各类组合
'''

import pandas as pd
import mylog as mylog
import numpy as np
from datetime import datetime,timedelta
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql


class ConstructPortfolio:
    def __init__(self):
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()

    def ConstructTotal(self,codeList,tradedate,factor,pastName='',rptFlag=False,divisiond100=False,divideMethod=0):
        '''
        基于单个因子的股票池划分
        :param codeList: 初始股票池
        :param tradedate: 当前查询因子的日期
        :param factor: 因子wind代码
        :param rptFlag: 是否报告期因子
        :param divisiond100: 取值后是否除以100
        :param divideMethon: 股票池划分方法标签（0：0.3，0.4，0.3划分，1：0.5，0.5划分）
        :param pastName:获取数据后的factor重命名
        :return: 字典对应股票池
        '''
        result = {}
        if not rptFlag:
            df = self.GetDataFromWindAndMySqlDemo.getFactorDailyData(codeList=codeList, factors=[factor],
                                                                     tradeDate=tradedate)
        else:

            df = self.GetDataFromWindAndMySqlDemo.getFactorReportData(codeList=codeList, factors=[factor],
                                                                       rptDate=tradedate)
            if factor == 'wgsd_assets':
                rptBackDate = str(int(tradedate[:4]) - 1) + tradedate[4:]
                df2 = self.GetDataFromWindAndMySqlDemo.getFactorReportData(codeList=codeList, factors=[factor],
                                                                           rptDate=rptBackDate)
                df = (df - df2) / df2

        if df.empty:
            return result

        if divisiond100:
            df = df/100

        if factor=='pb_lf':
            df = df[df > 0]
            df[factor] = 1 / df[factor]

        if pastName:
            df.rename(columns={factor:pastName}, inplace=True)
        df.dropna(inplace=True)

        if divideMethod==0:
            medianValue = np.percentile(df[pastName], 50)
            result['small'+pastName] = df[df[pastName] < medianValue].index.tolist()
            result['big'+pastName] = df[df[pastName] >= medianValue].index.tolist()
        else:
            threePer = np.percentile(df[pastName], 30)
            sevenPer = np.percentile(df[pastName], 70)
            result['low'+pastName] = df[df[pastName] <= threePer].index.tolist()
            result['middle'+pastName] = df[(sevenPer >= df[pastName]) & (df[pastName] > threePer)].index.tolist()
            result['high'+pastName] = df[df[pastName] > sevenPer].index.tolist()
        return result

    def ConstructWML(self,codeList,tradeDate):
        result = {}
        startDate = (datetime.strptime(tradeDate, "%Y-%m-%d") - timedelta(days=30 * 2)).strftime("%Y-%m-%d")
        endDate = (datetime.strptime(tradeDate, "%Y-%m-%d") - timedelta(days=30 * 0)).strftime("%Y-%m-%d")
        df = self.GetDataFromWindAndMySqlDemo.getPetChg(codeList=codeList,startDate=startDate,endDate=endDate)

        df.dropna(inplace=True)
        if df.empty:
            return result

        df.rename(columns={"pct_chg_value": "WML"}, inplace=True)
        df['WML'] = df['WML'] / 100

        threePer = np.percentile(df['WML'], 30)
        sevenPer = np.percentile(df['WML'], 70)
        result['Winner'] = df[df['WML'] <= threePer].index.tolist()
        result['MiddleTrade'] = df[(sevenPer >= df['WML']) & (df['WML'] > threePer)].index.tolist()
        result['Loser'] = df[df['WML'] > sevenPer].index.tolist()
        return result

    def ConstructAdjustStockPool(self, benchCode,tradeDate):
        totalStock = []
        # 初始股票池
        lastAnnualRptDate = str(int(tradeDate[:4]) - 1) + '-12-31'  # 离调仓当日最近的，年报披露日期
        df = self.GetDataFromWindAndMySqlDemo.getIndexConstituent(indexCode=benchCode,
                                                                  getDate=lastAnnualRptDate,
                                                                  indexOrSector='sector')
        if not df.empty:
            totalStock = df['stock_code'].tolist()
        return totalStock

if __name__ == "__main__":
    ConstructPortfolioDemo = ConstructPortfolio()
