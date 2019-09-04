# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    结果深度分析
'''

import pandas as pd
import mylog as mylog
from datetime import datetime,timedelta
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql
import numpy as np
import statsmodels.api as sm


class AnalyzeResult:
    def __init__(self):
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
        self.logger = mylog.logger
        self.middleDataPath = r"C:\\Users\\zouhao\\PycharmProjects\\QFactorModel\\MiddleData\\"

    def CalcFamaMonthReturn(self):
        qFactorFileName = self.middleDataPath + r'ResultData\\' + "fama万得全Amkt_cap_float&fa_roe_wgt&2010-2019&总市值加权2月old动量含金融.xlsx"

        portfolioDf = pd.read_excel(qFactorFileName, index_col=0)
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
        HMLPortfolio = highPBNamePortfolio - lowPBNamePortfolio
        HMLPortfolio.name = 'HML'

        winnerName = [name for name in portfolioNameList if name.find("Winner") != -1]
        loserName = [name for name in portfolioNameList if name.find("Loser") != -1]
        winnerPortfolio = portfolioDf[winnerName].mean(axis=1)
        loserPortfolio = portfolioDf[loserName].mean(axis=1)
        WMLPortfolio = winnerPortfolio - loserPortfolio
        WMLPortfolio.name = 'WML'

        totalDf = pd.concat([MEPortfolio, HMLPortfolio,WMLPortfolio], axis=1, sort=True)
        return totalDf

    def CalcQfatorMonthReturn(self):
        qFactorFileName = self.middleDataPath + r'ResultData\\' + "万得全A成分mkt_cap_ard&fa_roe_wgt&2010-2019&总市值加权不含金融银行.xlsx"
        portfolioDf = pd.read_excel(qFactorFileName, index_col=0)
        portfolioNameList = portfolioDf.columns.tolist()

        bigName = [name for name in portfolioNameList if name.find("bigSize") != -1]
        smallName = [name for name in portfolioNameList if name.find("smallSize") != -1]
        smallPortfolio = portfolioDf[smallName].mean(axis=1)
        bigPortfolio = portfolioDf[bigName].mean(axis=1)
        MEPortfolio = smallPortfolio - bigPortfolio
        MEPortfolio.name = 'ME'

        lowInvestName = [name for name in portfolioNameList if name.find("lowDeltaA") != -1]
        highInvestName = [name for name in portfolioNameList if name.find("highDeltaA") != -1]
        lowInvestPortfolio = portfolioDf[lowInvestName].mean(axis=1)
        highInvestPortfolio = portfolioDf[highInvestName].mean(axis=1)
        deletaPortfolio = lowInvestPortfolio - highInvestPortfolio
        deletaPortfolio.name = 'DeltaA'

        highROEName = [name for name in portfolioNameList if name.find("highROE") != -1]
        lowROEName = [name for name in portfolioNameList if name.find("lowROE") != -1]
        lowROEPortfolio = portfolioDf[lowROEName].mean(axis=1)
        highROEPortfolio = portfolioDf[highROEName].mean(axis=1)
        ROEPortfolio = highROEPortfolio - lowROEPortfolio
        ROEPortfolio.name = 'ROE'
        totalDf = pd.concat([MEPortfolio, deletaPortfolio, ROEPortfolio], axis=1, sort=True)
        return totalDf

    def GetFactorData(self):
        tradeStartDate= '2010-05-31'
        QFactorReturn = self.CalcQfatorMonthReturn()
        famaFactorReturn = self.CalcFamaMonthReturn()
        totalFactorReturn = pd.concat([QFactorReturn, famaFactorReturn], axis=1, join='inner', sort=True)

        benchCode = '000300.SH'
        totalDate = totalFactorReturn.index.tolist()

        startDate = (datetime.strptime(totalDate[0],"%Y-%m-%d")-timedelta(70)).strftime("%Y-%m-%d")
        benchDf = self.GetDataFromWindAndMySqlDemo.getHQData(tempCode=benchCode, startDate=startDate,
                                                             endDate=totalDate[-1], tableFlag='index')
        benchDf.rename(columns={"close_price": benchCode}, inplace=True)

        benchDf = benchDf.loc[[tradeStartDate]+totalFactorReturn.index.tolist()]
        benchReturn = benchDf / benchDf.shift(1) - 1

        dateList = benchReturn.index.tolist()
        riskDf = self.GetDataFromWindAndMySqlDemo.getRiskFree(startDate=startDate, endDate=dateList[-1]) / 12 / 100
        updateList = [dateStr.strftime("%Y-%m-%d") for dateStr in riskDf.index.tolist()]
        riskDf = pd.DataFrame(riskDf.values, index=updateList, columns=['Risk_Free']).fillna(method='pad')
        riskDf = riskDf.loc[benchReturn.index].fillna(method='pad')

        marketReturn = benchReturn[benchCode] - riskDf['Risk_Free']
        marketReturn.name = 'MKT'

        totalDf = pd.concat([totalFactorReturn, marketReturn], axis=1, sort=True).dropna()
        return totalDf


    def calcRegression(self,totalDf):
        targetY = ['ME','deleta','ROE']
        targetDicX = {}
        targetDicX['CAPM'] = ['MKT']
        targetDicX['FamaFrench'] = ['MKT','SMB','HML']
        targetDicX['Carhart'] = ['MKT','SMB','HML','WML']

        dfTotalList = []
        for modelName,xName in targetDicX.items():
            x = totalDf[xName].values.reshape(totalDf.shape[0], len(xName))
            X = np.hstack((np.ones((totalDf.shape[0], 1)), x))
            dfList = []
            for code in targetY:
                tempResultDic = {}
                Y = totalDf[code].values
                try:
                    res = (sm.OLS(Y,X)).fit()
                except:
                    a=0
                tempResultDic['RSquare'] = res.rsquared
                tempResultDic['ParamName'] = ['alpha']+xName
                tempResultDic['Coeff'] = res.params
                tempResultDic['Tvalues'] = res.tvalues
                tempResultDic['Pvalues'] = res.pvalues
                tempResultDic['AIC'] = res.aic
                tempResultDic['BIC'] = res.bic
                tempResultDic['FPvalue'] = res.f_pvalue
                tempResultDic['Fvalue'] = res.fvalue
                tempResultDic['RSquareAdj'] = res.fvalue
                tempResultDic['ModelName'] = modelName
                tempResultDic['Factor'] = code
                tempRegressionDf = pd.DataFrame(tempResultDic).set_index(['Factor','ModelName','ParamName'])
                dfList.append(tempRegressionDf)
            df = pd.concat(dfList,axis=0,sort=True)
            dfTotalList.append(df)
        totalRegressionDf = pd.concat(dfTotalList,axis=0,sort=True)
        totalRegressionDf.to_excel(self.middleDataPath+r'ResultData\\'+"Factor模型间回归.xlsx")

    def CalcDeepResult(self,totalDf):
        totalDf.describe().to_excel(self.middleDataPath+r'ResultData\\'+"Factor统计性描述.xlsx")
        corrDf = totalDf.corr()
        corrDf.to_excel(self.middleDataPath+r'ResultData\\'+"Factor相关性.xlsx")
        self.calcRegression(totalDf)

    def CalcMain(self):
        totalDf = self.GetFactorData()
        self.CalcDeepResult(totalDf)


if __name__ == "__main__":
    AnalyzeResultDemo = AnalyzeResult()
    AnalyzeResultDemo.CalcMain()
