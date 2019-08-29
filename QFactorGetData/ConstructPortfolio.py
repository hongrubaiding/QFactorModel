# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com


import pandas as pd
import mylog as mylog
import numpy as np
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql


class ConstructPortfolio:
    def __init__(self):
        self.GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()


    def ConstructME(self,codeList,tradedate):
        df = self.GetDataFromWindAndMySqlDemo.getFactorDailyData(codeList=codeList, factors=["mkt_cap_float"],
                                                                 tradeDate=tradedate)
        df.rename(columns={"mkt_cap_float": "Size"}, inplace=True)
        df.dropna(inplace=True)
        medianValue = np.percentile(df['Size'],50)
        result = {}
        result['smallSize'] = df[df['Size']<medianValue].index.tolist()
        result['bigSize'] = df[df['Size']>=medianValue].index.tolist()
        return result

    def ConstructSMB(self,codeList,rptDate):
        # df = self.GetDataFromWindAndMySqlDemo.getFactorReportData(codeList=codeList,factors=["wgsd_assets"],rptDate=rptDate)
        df = self.GetDataFromWindAndMySqlDemo.getFactorDailyData(codeList=codeList,factors=["mkt_cap_ard"],tradeDate=rptDate)
        df.rename(columns={"mkt_cap_ard":"Size"},inplace=True)
        df.dropna(inplace=True)
        df.sort_values(by='Size',inplace=True)
        result = {}
        result['smallSize'] = df.iloc[:int(df.shape[0] / 2)].index.tolist()
        result['bigSize'] = df.iloc[int(df.shape[0] / 2):].index.tolist()
        return result,df

    def ConstructDelataA(self,codeList,rptDate):
        df1 = self.GetDataFromWindAndMySqlDemo.getFactorReportData(codeList=codeList,factors=["wgsd_assets"],rptDate=rptDate)
        rptBackDate = str(int(rptDate[:4])-1)+rptDate[4:]
        df2 = self.GetDataFromWindAndMySqlDemo.getFactorReportData(codeList=codeList, factors=["wgsd_assets"],
                                                                   rptDate=rptBackDate)
        df = (df1-df2)/df2
        df.rename(columns={"wgsd_assets": "DeltaA"}, inplace=True)
        df.dropna(inplace=True)
        df.sort_values(by='DeltaA', inplace=True)

        result={}
        threePer = np.percentile(df['DeltaA'],30)
        sevenPer = np.percentile(df['DeltaA'], 70)
        result['lowInvest'] = df[df['DeltaA']<=threePer].index.tolist()
        result['middleInvest'] = df[(sevenPer>=df['DeltaA'])&(df['DeltaA']>threePer)].index.tolist()
        result['highInvest'] = df[df['DeltaA']>sevenPer].index.tolist()
        return result

    def ConstructROE(self,codeList,rptDate):
        df = self.GetDataFromWindAndMySqlDemo.getFactorDailyData(codeList=codeList,factors=["fa_roe_wgt"],tradeDate=rptDate)
        df.dropna(inplace=True)
        df.rename(columns={"fa_roe_wgt": "ROE"}, inplace=True)
        df.sort_values(by='ROE', inplace=True)

        result = {}
        threePer = np.percentile(df['ROE'], 30)
        sevenPer = np.percentile(df['ROE'], 70)
        result['lowROE'] = df[df['ROE'] <= threePer].index.tolist()
        result['middleROE'] = df[(sevenPer >= df['ROE']) & (df['ROE'] > threePer)].index.tolist()
        result['highROE'] = df[df['ROE'] > sevenPer].index.tolist()
        return result


    def ConstructHML(self,codeList, tradedate):
        df = self.GetDataFromWindAndMySqlDemo.getFactorDailyData(codeList=codeList, factors=["pb_lf"],
                                                                 tradeDate=tradedate)
        df.dropna(inplace=True)
        df =df[df>0]
        df.rename(columns={"pb_lf": "PB"}, inplace=True)
        df['PB']= 1/df['PB']
        df.dropna(inplace=True)

        result = {}
        threePer = np.percentile(df['PB'], 30)
        sevenPer = np.percentile(df['PB'], 70)
        result['lowPB'] = df[df['PB'] <= threePer].index.tolist()
        result['middlePB'] = df[(sevenPer >= df['PB']) & (df['PB'] > threePer)].index.tolist()
        result['highPB'] = df[df['PB'] > sevenPer].index.tolist()
        return result

    def ConstructWML(self):
        pass
