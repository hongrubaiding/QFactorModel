# -- coding: utf-8 --
# Author:zouhao
# email:1084848158@qq.com

'''
    获取股票池数据
'''

import pandas as pd
import mylog as mylog
from GetAndSaveWindData.GetDataFromWindAndMySql import GetDataFromWindAndMySql

class GetInitStockPool:
    def __init__(self):
        self.logger = mylog.logger

    def GetStockConstituent(self,getDate,benchCode = '000016.SH',):
        '''GetAndSaveWindData
        获取指数成分股
        '''
        GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
        df = GetDataFromWindAndMySqlDemo.getIndexConstituent(indexCode=benchCode,getDate=getDate,indexOrSector='index')
        return df



if __name__=="__main__":
    GetInitStockPoolDemo = GetInitStockPool()
    GetInitStockPoolDemo.GetStockConstituent(getDate='2019-08-20')
