# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com


import pandas as pd
import mylog as mylog
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql


class AnalyzeResult:
    def __init__(self):
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
        self.logger = mylog.logger
        self.middleDataPath = r"C:\\Users\\zouhao\\PycharmProjects\\QFactorModel\\MiddleData\\"

    def CalcFamaMonthReturn(self):
        qFactorFileName = self.middleDataPath + r'ResultData\\' + "fama万得全Amkt_cap_float&fa_roe_wgt&2010-2019&总市值加权含金融.xlsx"

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
        qFactorFileName = self.middleDataPath + r'ResultData\\' + "万得全A成分mkt_cap_float&fa_roe_wgt&2010-2019&总市值加权不含金融.xlsx"
        portfolioDf = pd.read_excel(qFactorFileName, index_col=0)
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

    def CalcWMLMonthReturn(self):
        qFactorFileName = self.middleDataPath + r'ResultData\\' + "WML万得全A成分mkt_cap_float&fa_roe_wgt&2010-2019&总市值加权不含金融.xlsx"
        portfolioDf = pd.read_excel(qFactorFileName, index_col=0)
        # portfolioNameList = portfolioDf.columns.tolist()

        # WinnerPortfolio = portfolioDf['Winner'].mean(axis=1)
        # LoserPortfolio = portfolioDf['Loser'].mean(axis=1)
        WMLPortfolio = portfolioDf['Winner'] - portfolioDf['Loser']
        WMLPortfolio.name = 'WML'
        return WMLPortfolio

    def GetFactorData(self):
        QFactorReturn = self.CalcQfatorMonthReturn()
        famaFactorReturn = self.CalcFamaMonthReturn()
        WMLFactorReturn = self.CalcWMLMonthReturn()
        totalFactorReturn = pd.concat([QFactorReturn, famaFactorReturn,WMLFactorReturn], axis=1, join='inner', sort=True)

        benchCode = '000300.SH'
        totalDate = totalFactorReturn.index.tolist()
        benchDf = self.GetDataFromWindAndMySqlDemo.getHQData(tempCode=benchCode, startDate=totalDate[0],
                                                             endDate=totalDate[-1], tableFlag='index')
        benchDf.rename(columns={"close_price": benchCode}, inplace=True)
        benchDf = benchDf.loc[totalFactorReturn.index]
        benchReturn = benchDf / benchDf.shift(1) - 1
        benchReturn = benchReturn

        dateList = benchReturn.index.tolist()
        riskDf = self.GetDataFromWindAndMySqlDemo.getRiskFree(startDate=dateList[0], endDate=dateList[-1]) / 12 / 100
        updateList = [dateStr.strftime("%Y-%m-%d") for dateStr in riskDf.index.tolist()]
        riskDf = pd.DataFrame(riskDf.values, index=updateList, columns=['Risk_Free']).fillna(method='pad')
        riskDf = riskDf.loc[benchReturn.index].fillna(method='pad')

        marketReturn = benchReturn[benchCode] - riskDf['Risk_Free']
        marketReturn.name = 'MKT'

        totalDf = pd.concat([totalFactorReturn, marketReturn], axis=1, sort=True)
        return totalDf

    def CalcDeepResult(self,totalDf):
        totalDf.describe().to_excel(self.middleDataPath+r'ResultData\\'+"Factor统计性描述.xlsx")
        corrDf = totalDf.corr()
        corrDf.to_excel(self.middleDataPath+r'ResultData\\'+"Factor相关性.xlsx")

    def CalcMain(self):
        totalDf = self.GetFactorData()
        self.CalcDeepResult(totalDf)


if __name__ == "__main__":
    AnalyzeResultDemo = AnalyzeResult()
    AnalyzeResultDemo.CalcMain()
