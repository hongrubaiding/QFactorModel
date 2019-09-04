# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    计算投资组合收益
'''

import pandas as pd
import mylog as mylog
import numpy as np
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql


class CalcPortfolioReturn:
    def __init__(self):
        self.logger = mylog.logger
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()

    def GetStockHQData(self, codeList, startDate, endDate):
        finalReturnDf = pd.DataFrame()
        totalDf = self.GetDataFromWindAndMySqlDemo.getMonthData(codeList, startDate, endDate)
        if totalDf.empty:
            return finalReturnDf

        totalDf.set_index("update_time", inplace=True)
        dffinal = []
        for stockCode, tempDf in totalDf.groupby(by='stock_code'):
            tempSe = tempDf['close_price']
            tempSe.name = stockCode
            dffinal.append(tempSe)

        finalDf = pd.concat(dffinal, axis=1, sort=True, join='inner')
        finalReturnDf = finalDf / finalDf.shift(1) - 1
        return finalReturnDf

    def calcPortfioReturn(self, resultDic, tradeDate, nextTradeDate, fifterKey=''):
        # 构建的组合，持有到下一个tradeDate,计算收益率
        dicFactorReturn = {}
        for factorKey, codeList in resultDic.items():
            if factorKey == fifterKey:
                continue

            self.logger.info("计算%s组合市值加权收益率" % factorKey)
            finalReturnDf = self.GetStockHQData(codeList=codeList, startDate=tradeDate, endDate=nextTradeDate)
            if finalReturnDf.empty:
                self.logger.error("getQFactorReturn获取股票行情数据有误，请检查")
                return pd.DataFrame()

            marketValueDf = self.GetDataFromWindAndMySqlDemo.getFactorDailyData(codeList=codeList,
                                                                                factors=["mkt_cap_ard"],
                                                                                tradeDate=tradeDate)

            if marketValueDf.empty:
                self.logger.error("calcPortfioReturn获取股票总市值数据有误，请检查")
                return pd.DataFrame()

            marketValueDf.rename(columns={"mkt_cap_ard": "stock_value"}, inplace=True)
            marketValueDf.dropna(inplace=True)
            try:
                usefulMarketDf = marketValueDf.loc[codeList]
            except:
                self.logger.error("calcPortfioReturn获取股票总市值为空值，请检查!", codeList)
                return pd.DataFrame()

            weight = usefulMarketDf / usefulMarketDf.sum()
            portfolio = (weight['stock_value'] * finalReturnDf[weight.index.tolist()]).sum(axis=1)

            tempResult = {nextTradeDate: portfolio[nextTradeDate]}
            dicFactorReturn[factorKey] = tempResult
        factorReturnDf = pd.DataFrame(dicFactorReturn)
        return factorReturnDf


if __name__ == "__main__":
    CalcPortfolioReturnDemo = CalcPortfolioReturn()
