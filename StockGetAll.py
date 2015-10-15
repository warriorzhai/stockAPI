 # -*- coding:gb2312 -*-
from stockdata_setting import Client
from StockInterface import StockInterface
from DatabaseInterface import DatabaseInterface
import csv
import json
import sys
import os
import datetime
import pymongo
import pandas as pd
import itertools as its
import time
os.chdir("C:\Users\saisn\Desktop\myfinance\stock data")



#XSHG表示上海证券交易所，XSHE表示深圳证券交易所，XHKG表示香港交易所，XNYS表示纽约证券交易所，NOBB表示三板市场，XNYS表示美国纳斯达克证券交易所




class StockGetAll(object):
    
    def __init__(self,token):
        self.token=token
        self.db_name='stock'
        self.stkif=StockInterface(token)
    
    #将一个字典列表中，所有指定key的值导出
    def unpack_dic(self,key,diclist):
        #key，指定键值，str
        #diclist，字典list，如[{dic1},{dic2}]
        
        #为防万一，使用如果key不存在于某个字段会报错
        try:
            res=[i[key] for i in diclist]
        except KeyError:
            print '字典中key值不存在'
            sys.exit()
        #此种方法即使key不存在于list中某个字典，会跳过这个字典继续输出
        #res=[i[key] for i in diclist if key in i]
        return res
        
    #获取当日日期的str格式
    def today_as_str(self,date_format="%Y%m%d"):
        now = datetime.datetime.now()
        todate=now.strftime(date_format)
        return todate
    
    #获取所有当日ticker列表
    #ticker满足以下条件：
    #1.交易所范围在exchages=['XSHG','XSHE']，即沪深两市
    #2.企业在上市状态
    #3.包含停牌企业
    #4.A股
    def get_api_tickers(self):
        key='ticker'
        fields=[key]
        res=self.stkif._getEqu(fields)
        tickers=self.unpack_dic(key,res)
        return tickers
    

    
    
    
    #获取currentinfo表所需数据
    def get_api_currentinfo(self):
        res=self.get_api_tickers()
        currentinfo={}
        currentinfo['currentTickerList']=res
        currentinfo['tickerDate']=None
        currentinfo['stockbaseDate']=None
        currentinfo['stockfundsDate']=None
        currentinfo['stocktradeDate']=None
        currentinfo['stocktradestep']=-1
        currentinfo['stockbasestep']=-1
        return [currentinfo]
    
            
    
    #获取stockbase表全部数据
    def get_api_stockbase_all(self,step):
        #获取当前全部股票代码
        db=DatabaseInterface()
        tickers=db.get_db_tickers()
        tickers=tickers[step+1:]
        return self.get_api_stockbase(tickers)
    
    #获取股票基本信息，登记stockbase
    #涉及3个接口：
        #1.getEqu，获取股票基本信息
        #2.getEquIndustry，获取行业信息
        #3.getSecTips，停牌复牌信息
    def get_api_stockbase(self,tickers):
        #定义接口的抓取fields
    
#        exchangeCD交易市场
#        ListSectorCD上市板块编码
#        ListSector上市板块
#        secShortName证券简称
#        listDate上市日期
#        equTypeCD股票分类编码
#        equType股票类别
#        totalShares总股本(最新)
#        nonrestFloatShares公司无限售流通股份合计(最新)
#        nonrestfloatA无限售流通股本(最新)。如果为A股，该列为最新无限售流通A股股本数量；如果为B股，该列为最新流通B股股本数量
#        primeOperating主营业务范围
        Equ_fields=['exchangeCD','ListSectorCD','ListSector','secShortName',
                    'listDate','equTypeCD','equType','totalShares',
                    'nonrestFloatShares','nonrestfloatA','primeOperating']
        #使用申银万国行业标准
        EquIndustry_fields=['industry','industryID1','industryID2',
        'industryID3','industryID4','industryName1','industryName2',
        'industryName3','industryName4']
        #获取tickers列表
        for t in tickers:
            Equ_dics=self.stkif._getEqu(Equ_fields,t)[0]
            EquIndustry_dics=self.stkif._getEquIndustry(t,EquIndustry_fields)[0]
            hub_dics=Equ_dics.copy()
            hub_dics.update(EquIndustry_dics)
            hub_dics['tipsTypeCD']='R'
            hub_dics['ticker']=t
            print t+'ticker get.........'
            yield hub_dics
    
    #获取全部股票复权交易信息,ts接口
    #1.ts复权数据接口
    #2.数据库stockbase三级行业ID信息
    def get_db_stocktradeadj(self,step,beginDate,endDate=''):
        #如果没有输入截止日期，就取得今天的日期
        if not endDate:
            endDate=self.today_as_str()
        #获取tickers列表
        db=DatabaseInterface()
        tickers=db.get_db_tickers()
        #获取上次中断步数
        tickers=tickers[step+1:]
        print 'started from ticker: '+tickers[0]
        
        
        col_name='stockbase'
        
        for t in tickers:
            ticker_trade={}
            ticker_trade['ticker']=t
            ticker_trade['tradedata']=self.stkif._getTradeDataAdj(t,beginDate=beginDate,endDate=endDate)
            ind=db.getone_db(self.db_name,col_name,{"ticker":t},['industryID3'])
            ticker_trade['industryID3']=ind['industryID3']
            print 'get ticker:'+t+'.....'
            yield ticker_trade
    
    def get_db_fundstocksinfo(self,beginYear,endYear=''):
        #如果没有输入截止年，就取得今年的年份
        if not endYear:
            endYear=int(self.today_as_str()[:4])
        years=range(beginYear,endYear+1)
        quaters=range(1,5)
        #循环取出起始年到截止年的所有是个季度的数据dataframe
        #如果季度数据还没有出来，就取得None
        frames=[self.stkif._getFundStocksData(y,q) for y,q in its.product(years,quaters)]
        #数据处理，处理成需要的数据库格式
        #数据拼合
        funddata=pd.concat(frames)
        #防止row index出现重复的情况
        m,n=funddata.shape
        funddata.index=range(m)
        print 'data combine finished......'
        #按照股票代码分组
        funddata_groups=funddata.groupby(by=funddata['code'])
        #每一组转成字典格式
        for code,df in funddata_groups:
            sub_funddata={}
            sub_funddata['ticker']=code
            sub_funddata['secShortName']=df.iloc[0,1]
            #获取每个日期的记录
            data=df.iloc[:,2:]
            sub_funddata['data']=json.loads(data.T.to_json()).values()
            yield sub_funddata
      
    #获取全部股票交易信息,通联接口
    #涉及1个接口：
        #1.getMktEqud，获取股票交易信息
        #2.getEquIndustry，获取行业信息
        #3.getSecTips，停牌复牌信息
    def get_db_stocktrade(token):
        now = datetime.datetime.now()
        todate=now.strftime("%Y%m%d")
        beginDate='20130101'
        ticker_fields=['secShortName','ticker','exchangeCD']
        industry_fields=['industry','industryID1','industryID2','industryID3','industryID4','industryName1','industryName2','industryName3','industryName4']
        stock_fields=['tradeDate','closePrice','negMarketValue','MarketValue','turnoverVol','turnoverValue','dealAmount','PE']
        ticker_gene=get_ticker_gene(MONGODB_DB,'stockbase',ticker_fields)
        if ticker_gene.count()==0:
            print '全部数据已经抓取完毕'
            sys.exit()
        else:
            print ticker_gene.count()
        connection = pymongo.MongoClient(
                MONGODB_URI,MONGODB_PORT
            )
        db = connection[MONGODB_DB]
        collection = db['stockbase']
        try:      
            for t in ticker_gene:
                try:
                    res_trade=_getMktEqud(token,t['ticker'],stock_fields,beginDate=beginDate,endDate=todate,tradeDate='')    
                    res_industry=_getEquIndustry(token,t['ticker'],industry_fields)
                except Exception, e:
                    raise e
                collection.update({"_id":t['_id']},{"$set":{"isupdate":1}})
                if res_trade is not None:
                    t.update(res_industry[0])
                    t['data']=res_trade
                    yield t
        except Exception, e:
            #traceback.print_exc()
            raise e
    
    def get_events(token):
        now = datetime.datetime.now()
        todate=now.strftime("%Y%m%d")
        beginDate='20130101'
        pg_fields=[]
        fh_fields=[]
        ticker_fields=['secShortName','ticker','exchangeCD']
        ticker_gene=get_ticker_gene(MONGODB_DB,'stockbase',ticker_fields)
        for t in ticker_gene:
                try:
                    #分红信息
                    res_fh=_getEquDiv(token,t['ticker'],fh_fields,beginDate,todate)  
                    #配股信息
                    res_pg=_getEquAllot(token,t['ticker'],pg_fields,beginDate,todate)
                except Exception, e:
                    raise e
                t['EquDiv']=res_fh
                t['EquAllot']=res_pg
                yield t








def main():
    token = '4648a6790ed5018390e3fe0d5d239bb4807cc762e645a08fe1bfba14dfc7ff70'
   
    #获取全部股票基本信息
    #res=_getSecID(token)
    #将股票基本信息写入数据库
    #write_db(MONGODB_DB,'stockbase',res['data'])
    #获取全部股票的交易数据
    #stockdata=get_stockdata(token)
    #获取全部事件数据
    eventdata=get_events(token)
    #将交易数据写入数据库
    write_db(MONGODB_DB,'stockevent',eventdata)
    #getMktEqud(client,ticker,beginDate='20140101',endDate=todate)



        
#test
#token = '4648a6790ed5018390e3fe0d5d239bb4807cc762e645a08fe1bfba14dfc7ff70'
#ticker='600633'
#fields=['closePrice','turnoverVol','turnoverValue','dealAmount','PE']
#beginDate='20150101'
#now = datetime.datetime.now()
#todate=now.strftime("%Y%m%d")
#res=_getMktEqud(token,ticker,fields,beginDate,todate)


