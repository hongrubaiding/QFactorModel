# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    构建q-factor因子模型
'''

import pandas as pd
import mylog as mylog
import numpy as np
from QFactorGetData.ConstructPortfolio import ConstructPortfolio
from QFactorGetData.CalcPortfolioReturn import CalcPortfolioReturn
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql
from QFactorGetData.CalcPortfolioReturn import CalcPortfolioReturn

class GetQFactorModel:
    def __init__(self):
        self.logger = mylog.logger
        self.ConstructPortfolioDemo = ConstructPortfolio()
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
        self.CalcPortfolioReturnDemo = CalcPortfolioReturn()

    def ExcludeFinancialFirms(self, codeList, tradeDate,totalIpoDf):
        '''
        排除金融行业(论文提到)，上市不足两年股票（delta用到历史两年年报数据）
        :return:
        '''
        stockIndustry = self.GetDataFromWindAndMySqlDemo.getBelongIndustry(codeList=codeList, tradeDate=tradeDate)
        if stockIndustry.empty:
            return ['ERROR']
        fifterCon = (stockIndustry['industry_name'] != '非银行金融') & (stockIndustry['industry_name'] != '银行')
        NotFinaceList = stockIndustry[fifterCon]['stock_code'].tolist()

        targetDf = totalIpoDf.loc[NotFinaceList]
        targetDate = str(int(tradeDate[:4]) - 2) + "-12-31"
        resultList = targetDf[targetDf['IpoDateStr'] < targetDate].index.tolist()
        return resultList

    def getMEDeltaPortfolio(self, totalStock, lastAnnualRptDate, tradedate):
        '''
        每年指定的月分（self.tradeDate）构建ME-->delta组合
        '''
        # 按照ME，划分股票池smallSize,bigSize
        resultSize = self.ConstructPortfolioDemo.ConstructTotal(codeList=totalStock, tradedate=tradedate,
                                                                factor='mkt_cap_ard', pastName='Size')
        if not resultSize:
            self.logger.error("getMEDelataPortfolio中，ConstructME构建组合为空，请检查")
            return resultSize

        resultSizeDelata = {}
        for sizekey, codeList in resultSize.items():
            tempDic = self.ConstructPortfolioDemo.ConstructTotal(codeList=codeList, tradedate=lastAnnualRptDate,
                                                                 factor='wgsd_assets', pastName='DeltaA',
                                                                 divideMethod=1, rptFlag=True)
            if not tempDic:
                continue
            for investKey, delataCodeList in tempDic.items():
                resultSizeDelata[sizekey + '-' + investKey] = delataCodeList
        return resultSizeDelata

    def getQFactorReturn(self, benchCode,totalTradeDate,totalIpoDf,tradeMonthDate='05-01'):
        dfList = []
        resultSizeDelata = {}
        for tradeDate in totalTradeDate:
            nextLoc = totalTradeDate.index(tradeDate) + 1
            if nextLoc >= len(totalTradeDate):
                break

            self.logger.info("获取%s各因子收益率" % tradeDate)
            if (not resultSizeDelata) or (tradeDate[5:7] == tradeMonthDate[:2]):
                totalStock = self.ConstructPortfolioDemo.ConstructAdjustStockPool(benchCode=benchCode,tradeDate=tradeDate)
                if not totalStock:
                    break

                # 过滤掉非银行金融，上市不满两年股票
                FifterCodeList = self.ExcludeFinancialFirms(codeList=totalStock, tradeDate=tradeDate,totalIpoDf=totalIpoDf)
                if 'ERROR' in FifterCodeList:
                    self.logger.error("过滤金融行业股票有有误，请检查")
                    break

                # 按照ME,delta,划分股票池
                lastAnnualRptDate = str(int(tradeDate[:4]) - 1) + '-12-31'
                resultSizeDelata = self.getMEDeltaPortfolio(totalStock=FifterCodeList,
                                                            lastAnnualRptDate=lastAnnualRptDate,
                                                            tradedate=tradeDate)

            if not resultSizeDelata:
                break

            resultSizeDelataROE = {}
            for SizeDelatakey, codeList in resultSizeDelata.items():
                tempDic = self.ConstructPortfolioDemo.ConstructTotal(codeList=codeList, tradedate=tradeDate,
                                                                     factor='fa_roe_wgt', pastName='ROE',
                                                                     divideMethod=1,divisiond100=True)

                if not tempDic:
                    continue

                for ROEKey, ROECodeList in tempDic.items():
                    resultSizeDelataROE[SizeDelatakey + '-' + ROEKey] = ROECodeList
            self.logger.info("%s,18个组合构建完成！" % tradeDate)

            if not resultSizeDelataROE:
                continue

            # 构建的组合，持有到下一个tradeDate,计算收益率
            nextTradeDate = totalTradeDate[totalTradeDate.index(tradeDate) + 1]
            factorReturnDf = self.CalcPortfolioReturnDemo.calcPortfioReturn(resultSizeDelataROE, tradeDate, nextTradeDate)
            dfList.append(factorReturnDf)
        resultDf = pd.concat(dfList, axis=0, sort=True)
        return resultDf

    def GetFactorMain(self, benchCode,totalTradeDate,totalIpoDf):
        resultDf = self.getQFactorReturn(benchCode,totalTradeDate,totalIpoDf)
        return resultDf

if __name__ == "__main__":
    GetQFactorModelDemo = GetQFactorModel()
    # GetQFactorModelDemo.GetFactorMain('000300.SH',['2010-05-31','2010-06-30'],totalIpoDf)