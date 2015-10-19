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



#XSHG��ʾ�Ϻ�֤ȯ��������XSHE��ʾ����֤ȯ��������XHKG��ʾ��۽�������XNYS��ʾŦԼ֤ȯ��������NOBB��ʾ�����г���XNYS��ʾ������˹���֤ȯ������




class StockGetAll(object):
    
    def __init__(self,token):
        self.token=token
        self.db_name='stock'
        self.stkif=StockInterface(token)
    
    #��һ���ֵ��б��У�����ָ��key��ֵ����
    def unpack_dic(self,key,diclist):
        #key��ָ����ֵ��str
        #diclist���ֵ�list����[{dic1},{dic2}]
        
        #Ϊ����һ��ʹ�����key��������ĳ���ֶλᱨ��
        try:
            res=[i[key] for i in diclist]
        except KeyError:
            print '�ֵ���keyֵ������'
            sys.exit()
        #���ַ�����ʹkey��������list��ĳ���ֵ䣬����������ֵ�������
        #res=[i[key] for i in diclist if key in i]
        return res
        
    #��ȡ�������ڵ�str��ʽ
    def today_as_str(self,date_format="%Y%m%d"):
        now = datetime.datetime.now()
        todate=now.strftime(date_format)
        return todate
    
    #��ȡ���е���ticker�б�
    #ticker��������������
    #1.��������Χ��exchages=['XSHG','XSHE']������������
    #2.��ҵ������״̬
    #3.����ͣ����ҵ
    #4.A��
    def get_api_tickers(self):
        key='ticker'
        fields=[key]
        res=self.stkif._getEqu(fields)
        tickers=self.unpack_dic(key,res)
        return tickers
    

    
    
    
    #��ȡcurrentinfo����������
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
    
            
    
    #��ȡstockbase��ȫ������
    def get_api_stockbase_all(self,step):
        #��ȡ��ǰȫ����Ʊ����
        db=DatabaseInterface()
        tickers=db.get_db_tickers()
        tickers=tickers[step+1:]
        return self.get_api_stockbase(tickers)
    
    #��ȡ��Ʊ������Ϣ���Ǽ�stockbase
    #�漰3���ӿڣ�
        #1.getEqu����ȡ��Ʊ������Ϣ
        #2.getEquIndustry����ȡ��ҵ��Ϣ
        #3.getSecTips��ͣ�Ƹ�����Ϣ
    def get_api_stockbase(self,tickers):
        #����ӿڵ�ץȡfields
    
#        exchangeCD�����г�
#        ListSectorCD���а�����
#        ListSector���а��
#        secShortName֤ȯ���
#        listDate��������
#        equTypeCD��Ʊ�������
#        equType��Ʊ���
#        totalShares�ܹɱ�(����)
#        nonrestFloatShares��˾��������ͨ�ɷݺϼ�(����)
#        nonrestfloatA��������ͨ�ɱ�(����)�����ΪA�ɣ�����Ϊ������������ͨA�ɹɱ����������ΪB�ɣ�����Ϊ������ͨB�ɹɱ�����
#        primeOperating��Ӫҵ��Χ
        Equ_fields=['exchangeCD','ListSectorCD','ListSector','secShortName',
                    'listDate','equTypeCD','equType','totalShares',
                    'nonrestFloatShares','nonrestfloatA','primeOperating']
        #ʹ�����������ҵ��׼
        EquIndustry_fields=['industry','industryID1','industryID2',
        'industryID3','industryID4','industryName1','industryName2',
        'industryName3','industryName4']
        #��ȡtickers�б�
        for t in tickers:
            Equ_dics=self.stkif._getEqu(Equ_fields,t)[0]
            EquIndustry_dics=self.stkif._getEquIndustry(t,EquIndustry_fields)[0]
            hub_dics=Equ_dics.copy()
            hub_dics.update(EquIndustry_dics)
            hub_dics['tipsTypeCD']='R'
            hub_dics['ticker']=t
            print t+'ticker get.........'
            yield hub_dics
    
    #��ȡȫ����Ʊ��Ȩ������Ϣ,ts�ӿ�
    #1.ts��Ȩ���ݽӿ�
    #2.���ݿ�stockbase������ҵID��Ϣ
    def get_db_stocktradeadj(self,step,beginDate,endDate=''):
        #���û�������ֹ���ڣ���ȡ�ý��������
        if not endDate:
            endDate=self.today_as_str()
        #��ȡtickers�б�
        db=DatabaseInterface()
        tickers=db.get_db_tickers()
        #��ȡ�ϴ��жϲ���
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
        #���û�������ֹ�꣬��ȡ�ý�������
        if not endYear:
            endYear=int(self.today_as_str()[:4])
        years=range(beginYear,endYear+1)
        quaters=range(1,5)
        #ѭ��ȡ����ʼ�굽��ֹ��������Ǹ����ȵ�����dataframe
        #����������ݻ�û�г�������ȡ��None
        frames=[self.stkif._getFundStocksData(y,q) for y,q in its.product(years,quaters)]
        #���ݴ����������Ҫ�����ݿ��ʽ
        #����ƴ��
        funddata=pd.concat(frames)
        #��ֹrow index�����ظ������
        m,n=funddata.shape
        funddata.index=range(m)
        print 'data combine finished......'
        #���չ�Ʊ�������
        funddata_groups=funddata.groupby(by=funddata['code'])
        #ÿһ��ת���ֵ��ʽ
        for code,df in funddata_groups:
            sub_funddata={}
            sub_funddata['ticker']=code
            sub_funddata['secShortName']=df.iloc[0,1]
            #��ȡÿ�����ڵļ�¼
            data=df.iloc[:,2:]
            sub_funddata['data']=json.loads(data.T.to_json()).values()
            yield sub_funddata
      
    #��ȡȫ����Ʊ������Ϣ,ͨ���ӿ�
    #�漰1���ӿڣ�
        #1.getMktEqud����ȡ��Ʊ������Ϣ
        #2.getEquIndustry����ȡ��ҵ��Ϣ
        #3.getSecTips��ͣ�Ƹ�����Ϣ
    def get_db_stocktrade(token):
        now = datetime.datetime.now()
        todate=now.strftime("%Y%m%d")
        beginDate='20130101'
        ticker_fields=['secShortName','ticker','exchangeCD']
        industry_fields=['industry','industryID1','industryID2','industryID3','industryID4','industryName1','industryName2','industryName3','industryName4']
        stock_fields=['tradeDate','closePrice','negMarketValue','MarketValue','turnoverVol','turnoverValue','dealAmount','PE']
        ticker_gene=get_ticker_gene(MONGODB_DB,'stockbase',ticker_fields)
        if ticker_gene.count()==0:
            print 'ȫ�������Ѿ�ץȡ���'
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
                    #�ֺ���Ϣ
                    res_fh=_getEquDiv(token,t['ticker'],fh_fields,beginDate,todate)  
                    #�����Ϣ
                    res_pg=_getEquAllot(token,t['ticker'],pg_fields,beginDate,todate)
                except Exception, e:
                    raise e
                t['EquDiv']=res_fh
                t['EquAllot']=res_pg
                yield t








def main():
    token = '4648a6790ed5018390e3fe0d5d239bb4807cc762e645a08fe1bfba14dfc7ff70'
   
    #��ȡȫ����Ʊ������Ϣ
    #res=_getSecID(token)
    #����Ʊ������Ϣд�����ݿ�
    #write_db(MONGODB_DB,'stockbase',res['data'])
    #��ȡȫ����Ʊ�Ľ�������
    #stockdata=get_stockdata(token)
    #��ȡȫ���¼�����
    eventdata=get_events(token)
    #����������д�����ݿ�
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


