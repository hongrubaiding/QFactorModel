# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    构建意向组合
'''

import pandas as pd
import mylog as mylog
import numpy as np
from datetime import datetime,timedelta
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql
from QFactorGetData.ConsturctAnomalies import ConstructAnomalies
from QFactorGetData.CalcPortfolioReturn import CalcPortfolioReturn
from QFactorGetData.ConstructPortfolio import ConstructPortfolio


class GetAnomalies:
    def __init__(self):
        self.logger = mylog.logger
        self.ConstructPortfolioDemo = ConstructPortfolio()
        self.ConstructAnomaliesDemo = ConstructAnomalies()

    def getExcludeIPOShort(self,codeList,tradeDate,totalIpoDf,days=int(365*1.5)):
        '''
        过滤掉上市时间距离回溯当月时间不足指定天数的股票
        :return:
        '''
        targetDf = totalIpoDf.loc[codeList]
        lastDate = (datetime.strptime(tradeDate,"%Y-%m-%d")-timedelta(days=days)).strftime("%Y-%m-%d")
        resultList = targetDf[targetDf['IpoDateStr'] <= lastDate].index.tolist()
        return resultList

    def GetSUEPortfolio(self,benchCode,totalTradeDate,totalIpoDf,tradeMonthDate='05-01'):
        CalcPortfolioReturnDemo = CalcPortfolioReturn()
        for tradeDate in totalTradeDate:
            nextLoc = totalTradeDate.index(tradeDate) + 1
            if nextLoc >= len(totalTradeDate):
                break

            self.logger.info("获取%sSUE异象收益率" % tradeDate)
            totalStock = self.ConstructPortfolioDemo.ConstructAdjustStockPool(benchCode=benchCode,
                                                                                  tradeDate=tradeDate)
            if not totalStock:
                break

            FifterCodeList = self.getExcludeIPOShort(codeList=totalStock,tradeDate=tradeDate,totalIpoDf=totalIpoDf)
            # resultSizeDelata = self.getMEDeltaPortfolio(totalStock=FifterCodeList,
            #                                             lastAnnualRptDate=lastAnnualRptDate,
            #                                             tradedate=tradeDate)


    def GetAnomaliesMain(self, benchCode,totalTradeDate,totalIpoDf):
        resultDf = self.GetSUEPortfolio(benchCode,totalTradeDate,totalIpoDf)
        return resultDf

if __name__ == "__main__":
    GetAnomaliesDemo = GetAnomalies()
    GetAnomalies.calcMain()