# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    数据手动导入，清洗
'''

import pandas as pd
import mylog as mylog
import numpy as np
from GetAndSaveWindData.GetDataToMysql import GetDataToMysql

class DataWash:
    def __init__(self):
        self.GetDataToMysqlDemo = GetDataToMysql()


    def getMain(self):
        dateStr = "2018-2019"

        #市净率导入
        # df = pd.read_excel("市净率%s.xlsx"%dateStr,index_col=[0])
        # dfList = []
        # for colName in df.columns:
        #     tempDf = df[[colName]].copy()
        #     tempDf['stock_code'] = tempDf.index.tolist()
        #     tempDf['item_wind_code'] = 'pb_lf'
        #     # tempDf['item_value'] = df[colName].tolist()
        #     tempDf['rpt_flag'] = [0]*tempDf.shape[0]
        #     tempDf['update_time'] = colName[-18:-8]
        #     tempDf.rename(columns={colName:"item_value"},inplace=True)
        #     dfList.append(tempDf)
        # totalDf = pd.concat(dfList,axis=0,sort=True)
        # self.GetDataToMysqlDemo.GetMain(totalDf, 'stock_factor_value')

        #总市值导入
        # df = pd.read_excel("总市值%s.xlsx" % dateStr, index_col=[0])
        # dfList = []
        # for colName in df.columns:
        #     tempDf = df[[colName]].copy()
        #     tempDf['stock_code'] = tempDf.index.tolist()
        #     tempDf['item_wind_code'] = 'mkt_cap_ard'
        #     # tempDf['item_value'] = df[colName].tolist()
        #     tempDf['rpt_flag'] = [0] * tempDf.shape[0]
        #     tempDf['update_time'] = colName[-18:-8]
        #     tempDf.rename(columns={colName: "item_value"}, inplace=True)
        #     dfList.append(tempDf)
        # totalDf = pd.concat(dfList, axis=0, sort=True)
        # self.GetDataToMysqlDemo.GetMain(totalDf, 'stock_factor_value')

        # 中信一级行业导入
        # df = pd.read_excel("中信行业%s.xlsx" % dateStr, index_col=[0])
        # dfList = []
        # for colName in df.columns:
        #     tempDf = df[[colName]].copy()
        #     tempDf['stock_code'] = tempDf.index.tolist()
        #     tempDf['industry_wind_code'] = 'industry_citic'
        #     tempDf['update_time'] = colName[-23:-13]
        #     tempDf['industry_flag'] = 1
        #     tempDf.rename(columns={colName: "industry_name"}, inplace=True)
        #     dfList.append(tempDf)
        # totalDf = pd.concat(dfList, axis=0, sort=True)
        # self.GetDataToMysqlDemo.GetMain(totalDf, 'stock_industry_value')

        # 总资产导入
        df = pd.read_excel("总资产.xlsx", index_col=[0])
        dfList = []
        for colName in df.columns:
            tempDf = df[[colName]].copy()
            tempDf['stock_code'] = tempDf.index.tolist()
            tempDf['item_wind_code'] = 'wgsd_assets'
            tempDf['rpt_flag'] = [1]*tempDf.shape[0]
            tempDf['update_time'] = colName[11:15]+'-12-31'
            tempDf.rename(columns={colName:"item_value"},inplace=True)
            dfList.append(tempDf)
        totalDf = pd.concat(dfList, axis=0, sort=True)
        self.GetDataToMysqlDemo.GetMain(totalDf, 'stock_factor_value')


if __name__=="__main__":
    DataWashDemo = DataWash()
    DataWashDemo.getMain()