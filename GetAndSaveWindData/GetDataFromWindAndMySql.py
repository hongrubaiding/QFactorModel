# -- coding: utf-8 --

'''
    将wind的数据导入到本地数据库,并从数据库返回结果
'''

from WindPy import w
import pandas as pd
import numpy as np
from GetAndSaveWindData.MysqlCon import MysqlCon
from GetAndSaveWindData.GetDataToMysql import GetDataToMysql
import mylog as mylog
w.start()

class GetDataFromWindAndMySql:
    def __init__(self):
        self.wsetData = ["000001.SH", "399300.SZ", "000016.SH", "000905.SH", "000906.SH"]  # 要获取数据的证券代码
        self.indexFieldName = ["open", "high", "low", "close", "volume", "amt", "chg", "pct_chg", "turn"]  # 要获取的数据字段
        self.fundFieldName = ["nav", "NAV_acc", "sec_name"]
        self.monetaryFund = ["mmf_annualizedyield", "mmf_unityield", "sec_name"]
        self.stockFieldName = ["open", "high", "low", "close", "volume", "amt", "turn", "mkt_cap_ard", "pe_ttm",
                               "ps_ttm", "pb_lf"]
        self.engine = MysqlCon().getMysqlCon(flag='engine')
        self.conn = MysqlCon().getMysqlCon(flag='connect')
        self.GetDataToMysqlDemo = GetDataToMysql()
        self.logger = mylog.logger

    def getBelongIndustry(self, codeList, tradeDate='2018-12-31'):
        '''
        获取股票所属的行业
        注：查询所属行业前，应确保查询日期大于股票ipo日期，该逻辑不在本方法验证。
        :return:
        '''
        sqlStr = "select * from stock_industry_value where stock_code in %s and update_time='%s'" % (
        tuple(codeList), tradeDate)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        if resultDf.empty:
            self.logger.debug("getBelongIndustry从wind获取！")
            tradeDateParam = tradeDate[:4] + tradeDate[5:7] + tradeDate[8:]
            wssData = w.wss(codes=codeList, fields=["industry_citic"],
                            options="tradeDate=%s;industryType=1" % tradeDateParam)
            if wssData.ErrorCode != 0:
                self.logger.error("获取指数成分股数据有误，错误代码" + str(wssData.ErrorCode))
                return pd.DataFrame()
            df = pd.DataFrame(wssData.Data, index=wssData.Fields, columns=wssData.Codes).T
            df.rename(columns={"INDUSTRY_CITIC": "industry_name"},inplace=True)
            df['stock_code'] = df.index.tolist()
            df['update_time'] = tradeDate
            df['industry_wind_code'] = ["industry_citic"] * df.shape[0]
            df['industry_flag'] = [1] * df.shape[0]
            self.GetDataToMysqlDemo.GetMain(df, 'stock_industry_value')
            resultDf = df[['stock_code', 'industry_name']]
        else:
            self.logger.debug("getBelongIndustry从本地数据库获取！")
            resultDf = resultDf[['stock_code', 'industry_name']]
        return resultDf

    def getFactorReportData(self, codeList, factors, rptDate='2018-12-31', backYears=0):
        # 单个年报数据的获取
        sqlStr = "select stock_code,item_value from stock_factor_value where stock_code in %s and update_time='%s' and item_wind_code='%s' " \
                 % (tuple(codeList), rptDate, factors[0])
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        if resultDf.empty:
            self.logger.debug("getFactorReportData从wind数据库获取%s！" % factors[0])
            rptDateParam = rptDate[:4] + rptDate[5:7] + rptDate[8:]
            if backYears == 0:
                if "wgsd_assets" not in factors:
                    wssData = w.wss(codes=codeList, fields=factors, options="rptDate=%s" % rptDateParam)
                else:
                    wssData = w.wss(codes=codeList, fields=factors,
                                    options="unit=1;rptDate=%s;rptType=1;currencyType=" % rptDateParam)
            else:
                wssData = w.wss(codes=codeList, fields=factors, options="rptDate=%s;N=%s" % (rptDateParam, str(backYears)))

            if wssData.ErrorCode != 0:
                self.logger.error("getFactorDailyData获取%s有误，错误代码%s"%(factors,str(wssData.ErrorCode)))
                return pd.DataFrame()
            df = pd.DataFrame(wssData.Data, columns=wssData.Codes, index=factors).T

            resultDf = df.copy()
            df['stock_code'] = df.index.tolist()
            df['update_time'] = rptDate
            df.rename(columns={factors: "item_value"},inplace=True)
            df['item_wind_code'] = factors[0]
            df['rpt_flag'] = 1
            self.GetDataToMysqlDemo.GetMain(df, 'stock_factor_value')
        else:
            self.logger.debug("getFactorReportData从本地数据库获取%s！"%factors[0])
            resultDf = resultDf.set_index("stock_code", drop=True).rename(columns={"item_value": factors[0]})
        return resultDf

    def getFactorDailyData(self, codeList, factors, tradeDate='2018-12-31'):
        #单个非年报数据的获取
        sqlStr = "select stock_code,item_value from stock_factor_value where stock_code in %s and update_time='%s' and item_wind_code='%s' "\
                 % (tuple(codeList),tradeDate,factors[0])
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        if resultDf.empty:
            self.logger.debug("getFactorDailyData从wind获取%s！" % factors[0])
            tradeDateParam = tradeDate[:4] + tradeDate[5:7] + tradeDate[8:]
            if 'mkt_cap_float' not in factors:
                wssData = w.wss(codes=codeList, fields=factors, options="tradeDate=%s" % tradeDateParam)
            else:
                wssData = w.wss(codes=codeList, fields=factors, options="unit=1;tradeDate=%s;currencyType=" % tradeDateParam)
            if wssData.ErrorCode != 0:
                self.logger.error("getFactorDailyData获取%s有误，错误代码%s" % (factors,str(wssData.ErrorCode)))
                return pd.DataFrame()
            df = pd.DataFrame(wssData.Data, index=factors, columns=wssData.Codes).T
            resultDf = df.copy()
            df['stock_code']= df.index.tolist()
            df['update_time'] = tradeDate
            df.rename(columns={factors[0]:"item_value"},inplace=True)
            df['item_wind_code'] = factors[0]
            df['rpt_flag'] = 0
            self.GetDataToMysqlDemo.GetMain(df, 'stock_factor_value')
        else:
            self.logger.debug("getFactorDailyData从本地数据库获取%s！" % factors[0])
            resultDf = resultDf.set_index("stock_code",drop=True).rename(columns={"item_value":factors[0]})
        return resultDf

    def getIndexConstituent(self, indexCode='000300.SH', getDate='2019-06-06', indexOrSector='index'):
        '''
        获取指数成分股
        '''
        sqlStr = "select * from index_constituent where index_code='%s' and update_time='%s'" % (indexCode, getDate)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        if resultDf.empty:
            self.logger.debug("getIndexConstituent从wind获取！")
            if indexOrSector == 'index':
                wsetdata = w.wset("indexconstituent", "date=%s;windcode=%s" % (getDate, indexCode))
            else:
                wsetdata = w.wset("sectorconstituent", "date=2019-08-21;windcode=%s" % indexCode)

            if wsetdata.ErrorCode != 0:
                self.logger.error("获取指数成分股数据有误，错误代码" + str(wsetdata.ErrorCode))
                return pd.DataFrame()

            resultDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields).T
            dateList = [datetampStr.strftime('%Y-%m-%d') for datetampStr in resultDf['date'].tolist()]
            resultDf['date'] = dateList
            nameDic = {'date': 'adjust_time', 'wind_code': 'stock_code', "sec_name": 'stock_name',
                       'i_weight': 'stock_weight'}
            resultDf.rename(columns=nameDic, inplace=True)
            resultDf['update_time'] = getDate
            resultDf['index_code'] = indexCode
            if 'stock_weight' not in resultDf.columns.tolist():
                resultDf['stock_weight'] = np.nan
            self.GetDataToMysqlDemo.GetMain(resultDf, 'index_constituent')

        else:
            self.logger.debug("getIndexConstituent从本地获取！")
        return resultDf

    def getLackDataToMySql(self, tempCode, startDate, endDate, tableFlag='index'):
        if tableFlag == 'index':
            tableStr = 'index_value'
            codeName = 'index_code'
        elif tableFlag == 'fund':
            tableStr = 'fund_net_value'
            codeName = 'fund_code'
        elif tableFlag == 'stock':
            tableStr = 'stock_hq_value'
            codeName = 'stock_code'
        elif tableFlag == 'private':
            return
        elif tableFlag == 'monetary_fund':
            tableStr = 'monetary_fund'
            codeName = 'fund_code'

        sqlStr = "select max(update_time),min(update_time) from %s where %s='%s'" % (tableStr, codeName, tempCode)
        cursor = self.conn.cursor()
        cursor.execute(sqlStr)
        dateStrTuple = cursor.fetchall()[0]
        maxDate = dateStrTuple[0]
        minDate = dateStrTuple[1]

        if not maxDate:
            self.getDataFromWind(tempCode, startDate=startDate, endDate=endDate, tableFlag=tableFlag)
            return

        if endDate < minDate or startDate > minDate:
            self.getDataFromWind(tempCode, startDate=startDate, endDate=endDate, tableFlag=tableFlag)
        elif startDate <= minDate:
            if minDate <= endDate < maxDate:
                if startDate != minDate:
                    self.getDataFromWind(tempCode, startDate=startDate, endDate=minDate, tableFlag=tableFlag)
            elif endDate >= maxDate:
                self.getDataFromWind(tempCode, startDate=startDate, endDate=minDate, tableFlag=tableFlag)
                if endDate != maxDate:
                    self.getDataFromWind(tempCode, startDate=maxDate, endDate=endDate, tableFlag=tableFlag)
        elif endDate > maxDate:
            self.getDataFromWind(tempCode, startDate=maxDate, endDate=endDate, tableFlag=tableFlag)

    def getDataFromWind(self, tempCode, startDate='2019-04-01', endDate='2019-04-30', tableFlag='index'):
        if tableFlag == 'index':
            tableStr = 'index_value'
            nameDic = {"OPEN": "open_price", "HIGH": "high_price", "LOW": "low_price", "CLOSE": "close_price",
                       "VOLUME": "volume", "AMT": "amt", "CHG": "chg", "PCT_CHG": "pct_chg", "TURN": "turn"}
            fields = self.indexFieldName
            codeName = 'index_code'
        elif tableFlag == 'fund':
            tableStr = 'fund_net_value'
            nameDic = {"NAV": "net_value", "NAV_ACC": "acc_net_value", "SEC_NAME": "fund_name"}
            fields = self.fundFieldName
            codeName = 'fund_code'
        elif tableFlag == 'stock':
            tableStr = 'stock_hq_value'
            nameDic = {"OPEN": "open_price", "HIGH": "high_price", "LOW": "low_price", "CLOSE": "close_price",
                       "VOLUME": "volume", "AMT": "amt", "TURN": "turn", "MKT_CAP_ARD": "market_value",
                       "PE_TTM": "pe_ttm", "PS_TTM": "ps_ttm", "PB_LF": "pb_lf"}
            fields = self.stockFieldName
            codeName = 'stock_code'
        elif tableFlag == 'monetary_fund':
            tableStr = 'monetary_fund'
            nameDic = {"MMF_ANNUALIZEDYIELD": "week_annual_return", "MMF_UNITYIELD": "wan_unit_return",
                       "SEC_NAME": "fund_name"}
            fields = self.monetaryFund
            codeName = 'fund_code'

        if tableFlag == 'stock':
            wsetdata = w.wsd(codes=tempCode, fields=fields, beginTime=startDate, endTime=endDate, options="PriceAdj=F")
        else:
            wsetdata = w.wsd(codes=tempCode, fields=fields, beginTime=startDate, endTime=endDate)

        if wsetdata.ErrorCode != 0:
            self.logger.error("获取行情数据有误，错误代码" + str(wsetdata.ErrorCode))
            return

        tempDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields, columns=wsetdata.Times).T
        tempDf.dropna(how='all', inplace=True)
        tempDf[codeName] = tempCode
        tempDf['update_time'] = tempDf.index.tolist()
        tempDf.rename(columns=nameDic, inplace=True)
        dateList = [dateStr.strftime("%Y-%m-%d") for dateStr in tempDf['update_time'].tolist()]
        tempDf['update_time'] = dateList
        self.GetDataToMysqlDemo.GetMain(tempDf, tableStr)
        return tempDf

    def getDataFromMySql(self, tempCode, startDate, endDate, tableFlag='index', nameList=['close_price']):
        if not nameList:
            self.logger.error('传入获取指数的字段不合法，请检查！')

        if tableFlag == 'index':
            tableStr = 'index_value'
            codeName = 'index_code'
        elif tableFlag == 'fund':
            codeName = 'fund_code'
            tableStr = 'fund_net_value'
        elif tableFlag == 'stock':
            codeName = 'stock_code'
            tableStr = 'stock_hq_value'
        elif tableFlag == 'private':
            codeName = 'fund_code'
            tableStr = 'private_net_value'
        elif tableFlag == 'monetary_fund':
            codeName = 'fund_code'
            tableStr = 'monetary_fund'

        sqlStr = "select %s,update_time from %s where %s='%s' and  update_time>='%s'" \
                 " and update_time<='%s'" % (','.join(nameList), tableStr, codeName, tempCode, startDate, endDate)
        resultDf = pd.read_sql(sql=sqlStr, con=self.engine)
        resultDf = resultDf.drop_duplicates('update_time').sort_index()
        resultDf.set_index(keys='update_time', inplace=True, drop=True)
        return resultDf

    def checkLackMonthData(self, tempDf,codeList):
        totalCodeList = list(tempDf['stock_code'].unique())
        lackCode = [code for code in codeList if code not in totalCodeList]
        haveCode = list(set(codeList).difference(lackCode))
        for code in haveCode:
            if tempDf[tempDf['stock_code']==code].shape[0]<2:
                lackCode.append(code)
        return lackCode

    def getMonthData(self, codeList=[], startDate='2019-03-01', endDate='2019-05-30'):
        totalTradeList = [startDate,endDate]
        sqlstr = "select * from stock_month_value where stock_code in %s and update_time in %s" % (
            tuple(codeList), tuple(totalTradeList))
        tempDf = pd.read_sql(sql=sqlstr, con=self.engine)
        lackCode = self.checkLackMonthData(tempDf,codeList)
        if lackCode:
            self.logger.debug("getMonthData从wind获取,缺失code: %s"%','.join(lackCode))
            dfList = []

            for tradeDate in [startDate,endDate]:
                tradeDateStr = tradeDate[:4] + tradeDate[5:7] + tradeDate[8:]
                wssData = w.wss(codes=lackCode, fields=['close', 'sec_name'],
                                options="tradeDate=%s;priceAdj=F;cycle=M" % tradeDateStr)
                if wssData.ErrorCode != 0:
                    self.logger.error("获取股票截面行情价格有误，错误代码" + str(wssData.ErrorCode))
                    return pd.DataFrame()
                df = pd.DataFrame(wssData.Data, columns=wssData.Codes, index=wssData.Fields).T
                df.rename(columns={"CLOSE": "close_price", "SEC_NAME": "stock_name"}, inplace=True)
                df['update_time'] = [tradeDate] * len(df)
                df['stock_code'] = df.index.tolist()
                dfList.append(df)
            tempLackDf = pd.concat(dfList, axis=0, sort=True)
            self.GetDataToMysqlDemo.GetMain(tempLackDf, 'stock_month_value')
            tempDf = pd.concat([tempDf,tempLackDf],axis=0,sort=True)
            tempDf = tempDf.drop_duplicates(subset=['stock_code','update_time'])
        else:
            self.logger.debug("getMonthData从本地数据库获取！")
        return tempDf[['stock_code', 'close_price', 'update_time']]

    def getHQData(self, tempCode, startDate='2019-03-01', endDate='2019-05-30', tableFlag='index',nameList=['close_price']):
        '''
        #获取指数行情数据入口
        '''
        self.getLackDataToMySql(tempCode, startDate, endDate, tableFlag)
        resultDf = self.getDataFromMySql(tempCode, startDate, endDate, tableFlag=tableFlag, nameList=nameList)
        return resultDf

    def getRiskFree(self, startDate='2019-03-01', endDate='2019-05-30'):
        wsetdata = w.wsd(codes=["SHI3MS1Y.IR"], fields=["close"], beginTime=startDate, endTime=endDate)
        if wsetdata.ErrorCode != 0:
            self.logger.error("获取行情数据有误，错误代码" + str(wsetdata.ErrorCode))
            return
        tempDf = pd.DataFrame(wsetdata.Data, index=wsetdata.Fields, columns=wsetdata.Times).T
        return tempDf

    def getTradeDay(self, startDate, endDate, Period=''):
        '''
        获取指定周期交易日,封装wind接口
        :param Period: ''日，W周，M月，Q季，S半年，Y年
        :return:
        '''
        # w.start()
        data = w.tdays(beginTime=startDate, endTime=endDate, options="Period=%s" % Period)
        if data.ErrorCode != 0:
            self.logger.error('wind获取交易日期错误，请检查！')
            return
        tradeDayList = data.Data[0]
        tradeDayList = [tradeDay.strftime('%Y-%m-%d') for tradeDay in tradeDayList]
        df = pd.DataFrame(tradeDayList, columns=['tradeDate'])
        df['startDate'] = [startDate] * df.shape[0]
        df['endDate'] = [endDate] * df.shape[0]
        df['Period'] = [Period] * df.shape[0]
        return df

if __name__ == '__main__':
    GetDataFromWindAndMySqlDemo = GetDataFromWindAndMySql()
    # GetDataFromWindAndMySqlDemo.getFactorReportData(codeList=["300033.SZ","601878.SH"],factors=["roe","profittogr"])
    GetDataFromWindAndMySqlDemo.getFactorDailyData(codeList=["300033.SZ", "601878.SH"], factors=["mkt_cap_ard"])
    aa = GetDataFromWindAndMySqlDemo.getHQData(tempCode='000300.SH', startDate='2019-02-01', endDate='2019-05-01')
    # aa = GetDataFromWindAndMySqlDemo.getIndexConstituent(indexCode='000905.SH',getDate='2010-02-03')
    # getHQData(self, tempCode, startDate='2019-04-01', endDate='2019-04-30', tableFlag='index',
    #           nameList=['close_price']):
    # aa = GetDataFromWindAndMySqlDemo.getHQData(tempCode='300033.SZ',tableFlag='stock',startDate='2010-01-01',endDate='2010-02-01')
    # aa = GetDataFromWindAndMySqlDemo.getCurrentDateData(tempCodeList=['300033.SZ','600000.SH'],getDate='2012-03-08')
    GetDataFromWindAndMySqlDemo.getMonthData()
    print(aa)
