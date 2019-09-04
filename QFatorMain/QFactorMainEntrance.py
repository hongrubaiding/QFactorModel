# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    主程序入口
'''

import pandas as pd
import mylog as mylog
from QFactorGetData.ConstructPortfolio import ConstructPortfolio
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql
from datetime import datetime, timedelta


class QFactorMainEntrance:
    def __init__(self):
        self.tradeDate = '05-01'
        self.startYear = '2010'
        self.endYear = '2019'
        self.logger = mylog.logger
        self.benchCode = '881001.WI'
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
        self.ConstructPortfolioDemo = ConstructPortfolio()
        self.middleDataPath = r"C:\\Users\\zouhao\\PycharmProjects\\QFactorModel\\MiddleData\\"
        totalIpoDf = pd.read_excel(self.middleDataPath + "ipodate.xlsx").set_index("stock_code", drop=True)
        totalIpoDf['IpoDateStr'] = [datestr.strftime("%Y-%m-%d") for datestr in totalIpoDf['ipo_date'].tolist()]
        self.totalIpoDf = totalIpoDf

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

    def ExcludeFinancialFirms(self, codeList, tradeDate):
        '''
        排除金融行业(论文提到)，上市不足两年股票（delta用到历史两年年报数据）
        :return:
        '''
        stockIndustry = self.GetDataFromWindAndMySqlDemo.getBelongIndustry(codeList=codeList, tradeDate=tradeDate)
        if stockIndustry.empty:
            return ['ERROR']
        fifterCon = (stockIndustry['industry_name'] != '非银行金融') & (stockIndustry['industry_name'] != '银行')
        NotFinaceList = stockIndustry[fifterCon]['stock_code'].tolist()

        targetDf = self.totalIpoDf.loc[NotFinaceList]
        targetDate = str(int(tradeDate[:4]) - 2) + "-12-31"
        resultList = targetDf[targetDf['IpoDateStr'] < targetDate].index.tolist()
        return resultList

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

    def getFamaFactorReturn(self, totalTradeDate):
        dfList = []
        resultSMBAndHML = {}
        for tradeDate in totalTradeDate:
            nextLoc = totalTradeDate.index(tradeDate) + 1
            if nextLoc >= len(totalTradeDate):
                break

            self.logger.info("获取%s各因子收益率" % tradeDate)
            if (not resultSMBAndHML) or (tradeDate[5:7] == self.tradeDate[:2]):
                totalStock = self.ConstructPortfolioDemo.ConstructAdjustStockPool(benchCode=self.benchCode,
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
            factorReturnDf = self.calcPortfioReturn(resultSMBAndHMLAndWML, tradeDate, nextTradeDate)
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
                totalStock = self.ConstructPortfolioDemo.ConstructAdjustStockPool(benchCode=self.benchCode,tradeDate=tradeDate)
                if not totalStock:
                    break

                # 过滤掉非银行金融，上市不满两年股票
                FifterCodeList = self.ExcludeFinancialFirms(codeList=totalStock, tradeDate=tradeDate)
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
            factorReturnDf = self.calcPortfioReturn(resultSizeDelataROE, tradeDate, nextTradeDate)
            dfList.append(factorReturnDf)
        resultDf = pd.concat(dfList, axis=0, sort=True)
        return resultDf

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

    def getTradeDay(self, Period='M'):
        startDate = self.startYear + '-' + self.tradeDate
        endDate = self.endYear + '-' + self.tradeDate
        fileName = "startDate=%s&endDate=%s;Period=%s.xlsx" % (startDate, endDate, Period)
        tradeDatePath = self.middleDataPath + r"TradeDate\\"
        try:
            tradeDf = pd.read_excel(tradeDatePath + fileName)
        except:
            tradeDf = self.GetDataFromWindAndMySqlDemo.getTradeDay(startDate=startDate, endDate=endDate, Period=Period)
            tradeDf.to_excel(tradeDatePath + fileName)

        totalTradeDate = tradeDf['tradeDate'].tolist()
        return totalTradeDate

    def calcMain(self):
        self.logger.info("程序开始...")

        totalTradeDate = self.getTradeDay()
        fileNameDir = self.middleDataPath + r'\\ResultData\\'

        # famaPortfolioDf = self.getFamaFactorReturn(totalTradeDate)
        # if famaPortfolioDf.empty:
        #     return

        # famaFileName = fileNameDir+"fama万得全Amkt_cap_float&fa_roe_wgt&%s-%s&总市值加权2月动量含金融.xlsx"%(self.startYear,self.endYear)
        # famaPortfolioDf.to_excel(famaFileName)

        portfolioDf = self.getQFactorReturn(totalTradeDate)
        if portfolioDf.empty:
            return
        qFactorFileName = fileNameDir + "万得全A成分mkt_cap_ard&fa_roe_wgt&%s-%s&总市值加权不含金融银行.xlsx" % (
            self.startYear, self.endYear)
        portfolioDf.to_excel(qFactorFileName)


if __name__ == '__main__':
    QFactorMainEntranceDemo = QFactorMainEntrance()
    QFactorMainEntranceDemo.calcMain()
