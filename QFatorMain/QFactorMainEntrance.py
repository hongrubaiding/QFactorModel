# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    主程序入口
'''

import pandas as pd
import mylog as mylog
import numpy as np
from datetime import datetime, timedelta
from QFactorGetData.ConstructPortfolio import ConstructPortfolio
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql
import os

class QFactorMainEntrance:
    def __init__(self):
        self.tradeDate = '05-01'
        self.startYear = '2010'
        self.endYear = '2019'
        self.logger = mylog.logger
        self.benchCode = '881001.WI'
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
        self.ConstructPortfolioDemo = ConstructPortfolio()
        self.filePath = os.getcwd()+r"\\MiddleResult\\"
        self.middleDataPath = r"C:\\Users\\zouhao\\PycharmProjects\\QFactorModel\\MiddleData\\"

    def getMEDeltaPortfolio(self, totalStock, lastAnnualRptDate, tradedate):
        '''
        每年指定的月分（self.tradeDate）构建ME-->delta组合
        '''
        #按照ME，划分股票池smallSize,bigSize
        resultSize = self.ConstructPortfolioDemo.ConstructME(codeList=totalStock, tradedate=tradedate)
        if not resultSize:
            self.logger.error("getMEDelataPortfolio中，ConstructME构建组合为空，请检查")
            return resultSize

        resultSizeDelata = {}
        for sizekey, codeList in resultSize.items():
            tempDic = self.ConstructPortfolioDemo.ConstructDelataA(codeList=codeList, rptDate=lastAnnualRptDate)
            if not tempDic:
               continue
            for investKey, delataCodeList in tempDic.items():
                resultSizeDelata[sizekey + '-' + investKey] = delataCodeList
        return resultSizeDelata

    def ExcludeFinancialFirms(self,codeList,tradeDate):
        '''
        排除金融行业(论文提到)，上市不足两年股票（delta用到历史两年年报数据）
        :return:
        '''
        stockIndustry = self.GetDataFromWindAndMySqlDemo.getBelongIndustry(codeList=codeList,tradeDate=tradeDate)
        if stockIndustry.empty:
            return ['ERROR']
        NotFinaceList = stockIndustry[stockIndustry['industry_name']!='非银行金融']['stock_code'].tolist()

        targetDf = self.totalIpoDf.loc[NotFinaceList]
        targetDate = str(int(tradeDate[:4])-2)+"-12-31"
        resultList = targetDf[targetDf['IpoDateStr']<targetDate].index.tolist()
        return resultList

    def getFFSMBAndHMLPortfolio(self, totalStock, tradedate):
        resultSMBHML = {}
        resultSMB = self.ConstructPortfolioDemo.ConstructME(codeList=totalStock, tradedate=tradedate)
        if not resultSMB:
            return resultSMBHML

        for sizekey, codeList in resultSMB.items():
            tempDic = self.ConstructPortfolioDemo.ConstructHML(codeList=codeList, tradedate=tradedate)
            if not tempDic:
                continue
            for investKey, delataCodeList in tempDic.items():
                resultSMBHML[sizekey + '-' + investKey] = delataCodeList
        return resultSMBHML

    def getFamaFactorReturn(self,totalTradeDate):
        dfList = []
        resultSMBAndHML={}
        for tradeDate in totalTradeDate:
            nextLoc = totalTradeDate.index(tradeDate) + 1
            if nextLoc >= len(totalTradeDate):
                break

            self.logger.info("获取%s各因子收益率" % tradeDate)
            if (not resultSMBAndHML) or (tradeDate[5:7] == self.tradeDate[:2]):
                lastAnnualRptDate = str(int(tradeDate[:4]) - 1) + '-12-31'  # 离调仓当日最近的，年报披露日期
                df = self.GetDataFromWindAndMySqlDemo.getIndexConstituent(indexCode=self.benchCode,
                                                                          getDate=lastAnnualRptDate,
                                                                          indexOrSector='sector')
                totalStock = df['stock_code'].tolist()
                resultSMBAndHML = self.getFFSMBAndHMLPortfolio(totalStock=totalStock,tradedate=tradeDate)
                self.logger.info("%s,6组合构建完成！" % tradeDate)

            if not resultSMBAndHML:
                continue

            # 构建的组合，持有到下一个tradeDate,计算收益率
            dicFactorReturn = {}
            nextTradeDate = totalTradeDate[totalTradeDate.index(tradeDate) + 1]
            for factorKey, codeList in resultSMBAndHML.items():
                self.logger.info("计算%s组合市值加权收益率" % factorKey)
                finalReturnDf = self.GetStockHQData(codeList=codeList, startDate=tradeDate,
                                                    endDate=nextTradeDate)
                if finalReturnDf.empty:
                    self.logger.error("getFamaFactorReturn获取股票行情数据有误，请检查")
                    return pd.DataFrame()

                marketValueDf = self.GetDataFromWindAndMySqlDemo.getFactorDailyData(codeList=codeList,
                                                                                    factors=["mkt_cap_ard"],
                                                                                    tradeDate=tradeDate)
                marketValueDf.rename(columns={"mkt_cap_ard": "stock_value"}, inplace=True)
                marketValueDf.dropna(inplace=True)
                try:
                    usefulMarketDf = marketValueDf.loc[codeList]
                except:
                    self.logger.error("getQFactorReturn获取股票总市值为空值，请检查!",codeList)
                    return pd.DataFrame()

                weight =usefulMarketDf / usefulMarketDf.sum()
                portfolio = (weight['stock_value'] * finalReturnDf[weight.index.tolist()]).sum(axis=1)

                tempResult = {nextTradeDate: portfolio[nextTradeDate]}
                dicFactorReturn[factorKey] = tempResult
            factorReturnDf = pd.DataFrame(dicFactorReturn)
            dfList.append(factorReturnDf)
        resultDf = pd.concat(dfList, axis=0, sort=True)
        return resultDf

    def getQFactorReturn(self, totalTradeDate):
        dfList = []
        for tradeDate in totalTradeDate:
            nextLoc = totalTradeDate.index(tradeDate) + 1
            if nextLoc >= len(totalTradeDate):
                break

            self.logger.info("获取%s各因子收益率" % tradeDate)
            if tradeDate[5:7] == self.tradeDate[:2]:
                #初始股票池
                lastAnnualRptDate = str(int(tradeDate[:4]) - 1) + '-12-31'  # 离调仓当日最近的，年报披露日期
                df = self.GetDataFromWindAndMySqlDemo.getIndexConstituent(indexCode=self.benchCode,
                                                                          getDate=lastAnnualRptDate,
                                                                          indexOrSector='sector')
                if df.empty:
                    break
                totalStock = df['stock_code'].tolist()

                #过滤掉非银行金融，上市不满两年股票
                FifterCodeList = self.ExcludeFinancialFirms(codeList=totalStock,tradeDate=tradeDate)
                if 'ERROR' in FifterCodeList:
                    self.logger.error("过滤金融行业股票有有误，请检查")
                    break

                #按照ME,delta,划分股票池
                resultSizeDelata = self.getMEDeltaPortfolio(totalStock=FifterCodeList, lastAnnualRptDate=lastAnnualRptDate,
                                                             tradedate=tradeDate)

            if not resultSizeDelata:
                break

            resultSizeDelataROE = {}
            for SizeDelatakey, codeList in resultSizeDelata.items():
                tempDic = self.ConstructPortfolioDemo.ConstructROE(codeList=codeList, rptDate=tradeDate)
                if not tempDic:
                    continue

                for ROEKey, ROECodeList in tempDic.items():
                    resultSizeDelataROE[SizeDelatakey + '-' + ROEKey] = ROECodeList
            self.logger.info("%s,18个组合构建完成！"%tradeDate)

            if not resultSizeDelataROE:
                continue

            # 构建的组合，持有到下一个tradeDate,计算收益率
            dicFactorReturn = {}
            nextTradeDate = totalTradeDate[totalTradeDate.index(tradeDate) + 1]
            for factorKey, codeList in resultSizeDelataROE.items():
                self.logger.info("计算%s组合市值加权收益率" % factorKey)
                finalReturnDf = self.GetStockHQData(codeList=codeList, startDate=tradeDate, endDate=nextTradeDate)
                if finalReturnDf.empty:
                    self.logger.error("getQFactorReturn获取股票行情数据有误，请检查")
                    return pd.DataFrame()

                marketValueDf = self.GetDataFromWindAndMySqlDemo.getFactorDailyData(codeList=codeList,
                                                                                    factors=["mkt_cap_ard"],
                                                                                    tradeDate=tradeDate)

                if marketValueDf.empty:
                    self.logger.error("getQFactorReturn获取股票总市值数据有误，请检查")
                    return pd.DataFrame()

                marketValueDf.rename(columns={"mkt_cap_ard": "stock_value"}, inplace=True)
                marketValueDf.dropna(inplace=True)
                try:
                    usefulMarketDf = marketValueDf.loc[codeList]
                except:
                    self.logger.error("getQFactorReturn获取股票总市值为空值，请检查!",codeList)
                    return pd.DataFrame()

                weight =usefulMarketDf / usefulMarketDf.sum()
                portfolio = (weight['stock_value'] * finalReturnDf[weight.index.tolist()]).sum(axis=1)

                tempResult = {nextTradeDate: portfolio[nextTradeDate]}
                dicFactorReturn[factorKey] = tempResult
            factorReturnDf = pd.DataFrame(dicFactorReturn)
            dfList.append(factorReturnDf)
        resultDf = pd.concat(dfList, axis=0, sort=True)
        return resultDf

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

    def CalcFamaMonthReurn(self,portfolioDf=pd.DataFrame()):
        portfolioDf = pd.read_excel("fama万得全Amkt_cap_float&fa_roe_wgt&2010-2019&总市值加权含金融.xlsx", index_col=0)
        portfolioNameList = portfolioDf.columns.tolist()

        bigName = [name for name in portfolioNameList if name.find("bigSize") != -1]
        smallName = [name for name in portfolioNameList if name.find("smallSize") != -1]
        smallPortfolio = portfolioDf[smallName].mean(axis=1)
        bigPortfolio = portfolioDf[bigName].mean(axis=1)
        MEPortfolio = smallPortfolio - bigPortfolio
        MEPortfolio.name = 'SMB'

        lowPBName = [name for name in portfolioNameList if name.find("lowPB") != -1]
        highPBName = [name for name in portfolioNameList if name.find("highPB") != -1]
        lowPBNamePortfolio = portfolioDf[lowPBName].mean(axis=1)
        highPBNamePortfolio = portfolioDf[highPBName].mean(axis=1)
        HMLPortfolio = highPBNamePortfolio-lowPBNamePortfolio
        HMLPortfolio.name = 'HML'

        totalDf = pd.concat([MEPortfolio, HMLPortfolio], axis=1, sort=True)
        return totalDf

    def CalcQfatorMonthReturn(self, portfolioDf=pd.DataFrame()):
        portfolioDf = pd.read_excel("万得全A成分mkt_cap_float&fa_roe_wgt&2010-2019&总市值加权不含金融.xlsx", index_col=0)
        portfolioNameList = portfolioDf.columns.tolist()

        bigName = [name for name in portfolioNameList if name.find("bigSize") != -1]
        smallName = [name for name in portfolioNameList if name.find("smallSize") != -1]
        smallPortfolio = portfolioDf[smallName].mean(axis=1)
        bigPortfolio = portfolioDf[bigName].mean(axis=1)
        MEPortfolio = smallPortfolio - bigPortfolio
        MEPortfolio.name = 'ME'

        lowInvestName = [name for name in portfolioNameList if name.find("lowInvest") != -1]
        highInvestName = [name for name in portfolioNameList if name.find("highInvest") != -1]
        lowInvestPortfolio = portfolioDf[lowInvestName].mean(axis=1)
        highInvestPortfolio = portfolioDf[highInvestName].mean(axis=1)
        deletaPortfolio = lowInvestPortfolio - highInvestPortfolio
        deletaPortfolio.name = 'deleta'

        highROEName = [name for name in portfolioNameList if name.find("highROE") != -1]
        lowROEName = [name for name in portfolioNameList if name.find("lowROE") != -1]
        lowROEPortfolio = portfolioDf[lowROEName].mean(axis=1)
        highROEPortfolio = portfolioDf[highROEName].mean(axis=1)
        ROEPortfolio = highROEPortfolio - lowROEPortfolio
        ROEPortfolio.name = 'ROE'
        totalDf = pd.concat([MEPortfolio, deletaPortfolio, ROEPortfolio], axis=1, sort=True)
        return totalDf

    def getTradeDay(self,Period='M'):
        startDate = self.startYear +'-' +self.tradeDate
        endDate = self.endYear + '-' +self.tradeDate
        fileName = "startDate=%s&endDate=%s;Period=%s.xlsx"%(startDate,endDate,Period)
        tradeDatePath = self.middleDataPath+r"TradeDate\\"
        try:
            tradeDf = pd.read_excel(tradeDatePath+fileName)
        except:
            tradeDf = self.GetDataFromWindAndMySqlDemo.getTradeDay(startDate=startDate,endDate=endDate, Period=Period)
            tradeDf.to_excel(tradeDatePath+fileName)

        totalTradeDate = tradeDf['tradeDate'].tolist()
        return totalTradeDate

    def calcMain(self):
        self.logger.info("程序开始...")
        totalIpoDf = pd.read_excel(self.middleDataPath + "ipodate.xlsx").set_index("stock_code",drop=True)

        totalIpoDf['IpoDateStr'] = [datestr.strftime("%Y-%m-%d") for datestr in totalIpoDf['ipo_date'].tolist()]
        self.totalIpoDf = totalIpoDf
        totalTradeDate = self.getTradeDay()

        famaPortfolioDf = self.getFamaFactorReturn(totalTradeDate)
        if famaPortfolioDf.empty:
            return
        famaPortfolioDf.to_excel("fama万得全Amkt_cap_float&fa_roe_wgt&%s-%s&总市值加权含金融.xlsx"%(self.startYear,self.endYear))

        portfolioDf = self.getQFactorReturn(totalTradeDate)
        if portfolioDf.empty:
            return
        portfolioDf.to_excel("万得全A成分mkt_cap_float&fa_roe_wgt&%s-%s&总市值加权不含金融.xlsx"%(self.startYear,self.endYear))
        # self.CalcQfatorMonthRetrn(portfolioDf)

    def totalFactor(self):
        QFactorReturn = self.CalcQfatorMonthReturn()
        famaFactorReturn = self.CalcFamaMonthReurn()
        totalFactorReturn = pd.concat([QFactorReturn,famaFactorReturn],axis=1,join='inner',sort=True)

        benchCode = '000300.SH'
        totalDate = totalFactorReturn.index.tolist()
        benchDf = self.GetDataFromWindAndMySqlDemo.getHQData(tempCode=benchCode,startDate=totalDate[0],
                                                             endDate=totalDate[-1], tableFlag='index')
        benchDf.rename(columns={"close_price": benchCode}, inplace=True)
        benchDf = benchDf.loc[totalFactorReturn.index]
        benchReturn = benchDf / benchDf.shift(1) - 1
        benchReturn = benchReturn

        dateList = benchReturn.index.tolist()
        riskDf = self.GetDataFromWindAndMySqlDemo.getRiskFree(startDate=dateList[0],endDate=dateList[-1])/12/100
        updateList = [dateStr.strftime("%Y-%m-%d") for dateStr in riskDf.index.tolist()]
        riskDf = pd.DataFrame(riskDf.values,index=updateList,columns=['Risk_Free']).fillna(method='pad')
        riskDf = riskDf.loc[benchReturn.index].fillna(method='pad')

        marketReturn = benchReturn[benchCode]-riskDf['Risk_Free']
        marketReturn.name = 'MKT'

        totalDf = pd.concat([totalFactorReturn, marketReturn], axis=1, sort=True)
        corrDf = totalDf.corr()

if __name__ == '__main__':
    QFactorMainEntranceDemo = QFactorMainEntrance()
    # QFactorMainEntranceDemo.calcMain()
    QFactorMainEntranceDemo.totalFactor()

