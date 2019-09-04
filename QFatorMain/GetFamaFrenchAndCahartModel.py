# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    构建FamaFrench三因子,Cahart四因子模型
'''

import pandas as pd
import mylog as mylog
import numpy as np
from QFactorGetData.ConstructPortfolio import ConstructPortfolio
from QFactorGetData.CalcPortfolioReturn import CalcPortfolioReturn


class GetFamaFrenchAndCahartModel:
    def __init__(self):
        self.ConstructPortfolioDemo = ConstructPortfolio()
        self.logger = mylog.logger

    def getFFSMBAndHMLPortfolio(self, totalStock, tradedate):
        resultSMBHML = {}
        resultSMB = self.ConstructPortfolioDemo.ConstructTotal(codeList=totalStock, tradedate=tradedate,
                                                               factor='mkt_cap_ard', pastName='Size')
        if not resultSMB:
            return resultSMBHML

        for sizekey, codeList in resultSMB.items():
            tempDic = self.ConstructPortfolioDemo.ConstructTotal(codeList=codeList, tradedate=tradedate,
                                                                 factor='pb_lf', pastName='PB', divideMethod=1,
                                                                 divisiond100=True)
            if not tempDic:
                continue
            for investKey, delataCodeList in tempDic.items():
                resultSMBHML[sizekey + '-' + investKey] = delataCodeList
        return resultSMBHML

    def getFamaFactorReturn(self, benchCode,totalTradeDate,tradeMonthDate='05-01'):
        CalcPortfolioReturnDemo =  CalcPortfolioReturn()
        dfList = []
        resultSMBAndHML = {}
        for tradeDate in totalTradeDate:
            nextLoc = totalTradeDate.index(tradeDate) + 1
            if nextLoc >= len(totalTradeDate):
                break

            self.logger.info("获取%s各因子收益率" % tradeDate)
            if (not resultSMBAndHML) or (tradeDate[5:7] == tradeMonthDate[:2]):
                totalStock = self.ConstructPortfolioDemo.ConstructAdjustStockPool(benchCode=benchCode,
                                                                                  tradeDate=tradeDate)
                if not totalStock:
                    break

                resultSMBAndHML = self.getFFSMBAndHMLPortfolio(totalStock=totalStock, tradedate=tradeDate)
                self.logger.info("%s,6组合构建完成！" % tradeDate)

            if not resultSMBAndHML:
                continue

            resultSMBAndHMLAndWML = {}
            nextTradeDate = totalTradeDate[totalTradeDate.index(tradeDate) + 1]
            for SMBHMLkey, codeList in resultSMBAndHML.items():
                tempDic = self.ConstructPortfolioDemo.ConstructWML(codeList=codeList, tradeDate=tradeDate)
                if not tempDic:
                    continue

                for WMLKey, WMLCodeList in tempDic.items():
                    resultSMBAndHMLAndWML[SMBHMLkey + '-' + WMLKey] = WMLCodeList
            self.logger.info("%s,18个组合构建完成！" % tradeDate)

            # 构建的组合，持有到下一个tradeDate,计算收益率
            factorReturnDf = CalcPortfolioReturnDemo.calcPortfioReturn(resultSMBAndHMLAndWML, tradeDate, nextTradeDate)
            dfList.append(factorReturnDf)
        resultDf = pd.concat(dfList, axis=0, sort=True)
        return resultDf

    def GetFactorMain(self, benchCode,totalTradeDate):
        resultDf = self.getFamaFactorReturn(benchCode,totalTradeDate)
        return resultDf


if __name__ == "__main__":
    GetFamaFrenchAndCahartModelDemo = GetFamaFrenchAndCahartModel()
    GetFamaFrenchAndCahartModelDemo.GetFactorMain('000300.SH',['2010-05-31','2010-06-30'])
