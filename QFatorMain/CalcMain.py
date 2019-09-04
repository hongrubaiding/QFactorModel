# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    程序主入口
'''

import pandas as pd
import numpy as np
from QFatorMain.GetFamaFrenchAndCahartModel import GetFamaFrenchAndCahartModel
from QFatorMain.GetQFactorModel import GetQFactorModel
import mylog as mylog
from QFactorGetData.ConstructPortfolio import ConstructPortfolio
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql


class CalcMain:
    def __init__(self):
        self.tradeDate = '05-01'
        self.startYear = '2010'
        self.endYear = '2011'
        self.logger = mylog.logger
        self.benchCode = '881001.WI'
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
        self.ConstructPortfolioDemo = ConstructPortfolio()
        self.middleDataPath = r"C:\\Users\\zouhao\\PycharmProjects\\QFactorModel\\MiddleData\\"
        totalIpoDf = pd.read_excel(self.middleDataPath + "ipodate.xlsx").set_index("stock_code", drop=True)
        totalIpoDf['IpoDateStr'] = [datestr.strftime("%Y-%m-%d") for datestr in totalIpoDf['ipo_date'].tolist()]
        self.totalIpoDf = totalIpoDf

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

    def GetStartMain(self):
        self.logger.info("程序开始...")
        totalTradeDate = self.getTradeDay()
        fileNameDir = self.middleDataPath + r'\\ResultData\\'

        GetFamaFrenchAndCahartModelDemo = GetFamaFrenchAndCahartModel()
        fFAndCahartDf = GetFamaFrenchAndCahartModelDemo.GetFactorMain(benchCode=self.benchCode,totalTradeDate=totalTradeDate)
        famaFileName = fileNameDir+"fama万得全Amkt_cap_float&fa_roe_wgt&%s-%s&总市值加权2月动量含金融.xlsx"%(self.startYear,self.endYear)
        fFAndCahartDf.to_excel(famaFileName)

        GetQFactorModelDemo = GetQFactorModel()
        qFactorDf = GetQFactorModelDemo.GetFactorMain(benchCode=self.benchCode,totalTradeDate=totalTradeDate,totalIpoDf=self.totalIpoDf)
        qFactorFileName = fileNameDir + "万得全A成分mkt_cap_ard&fa_roe_wgt&%s-%s&总市值加权不含金融银行.xlsx" % (
            self.startYear, self.endYear)
        qFactorDf.to_excel(qFactorFileName)

if __name__ == "__main__":
    CalcMainDemo = CalcMain()
    CalcMainDemo.GetStartMain()