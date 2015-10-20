# -*- coding: utf-8 -*-
"""
Created on Wed Aug 05 09:37:10 2015

@author: saisn
"""
import sys
import os
import datetime
import pymongo
from stockdata_setting import Client
from StockInterface import StockInterface
from DatabaseInterface import DatabaseInterface
from StockGetAll import StockGetAll
from StockDailyUpdate import StockDailyUpdate


class StockMain(object):
    def __init__(self):
        self.token='***'
        self.db_name='stock'
    
    
    def write_df_csv(self,df,file_name,data):
        df.to_csv(file_name,encoding='utf-8')
    
    def write_csv(self,filename,data,header=[]):
        fp=open(filename+'.csv', 'w', newline='')
        for dts in data:  
            wt = csv.writer(fp, delimiter=',')
            wt.writerows(data)
        fp.close()
    
    #获取当日日期的str格式
    def today_as_str(self,date_format="%Y%m%d"):
        now = datetime.datetime.now()
        todate=now.strftime(date_format)
        return todate
    
    #获取抓取进度，断点续传
    def get_step(self,key):
        db=DatabaseInterface()
        col_name='currentinfo'
        clause={}
        field=[key]
        res=db.getone_db(self.db_name,col_name,clause,field)
        return res[key]
    
    def write_currentinfo(self):
        col_name='currentinfo'
        api=StockGetAll(self.token)
        db=DatabaseInterface()
        db.drop_db_docs(self.db_name,col_name)
        currentinfo_data=api.get_api_currentinfo()
        db.write_db_withlog(self.db_name,col_name,currentinfo_data)
        db.update_date(self.db_name,'ticker')
    
    def write_stockbase(self):
        col_name='stockbase'
        api=StockGetAll(self.token)
        db=DatabaseInterface()
        #获取上次中断步数
        key=col_name+'step'
        step=self.get_step(key)
        #如果存在中断，就继续抓取；
        #如果不存在中断，数据库重抓，清空历史数据
        if step==-1:
            db.drop_db_docs(self.db_name,col_name)
            print col_name+' all clear!'
        print 'scraping from'+str(step+1)+' step....'
        data=api.get_api_stockbase_all(step)
        db.write_db_withlog(self.db_name,col_name,data)
        
        db.update_db_date(self.db_name,col_name)
    
    
    def write_stocktradeadj(self,beginDate,endDate=''):
        col_name='stocktrade'
        api=StockGetAll(self.token)
        db=DatabaseInterface()
        #获取上次中断步数
        key=col_name+'step'
        step=self.get_step(key)
        #如果存在中断，就继续抓取；
        #如果不存在中断，数据库重抓，清空历史数据
        if step==-1:
            db.drop_db_docs(self.db_name,col_name)
            print col_name+' all clear!'
        print 'scraping from'+str(step+1)+' step....'
        data=api.get_db_stocktradeadj(step,beginDate=beginDate,endDate=endDate)
        db.write_db_withlog(self.db_name,col_name,data)
        
        db.update_db_date(self.db_name,col_name)
    
    def write_revenuplan(self,year,top=100):
        today=self.today_as_str()
        file_name=str(year)+'revenu plan'+today+'.csv'
        api=StockInterface(self.token)
        res=api._getRevenuData(self,year=year,top=top)
        self.write_df_csv(self,res,file_name)
        
    def write_fundstockinfo(self,beginYear,endYear=''):
        col_name='stockfunds'
        api=StockGetAll(self.token)
        db=DatabaseInterface()
        db.drop_db_docs(self.db_name,col_name)
        data=api.get_db_fundstocksinfo(beginYear=beginYear,endYear=endYear)
        
    def daily_update(self):
        up=StockDailyUpdate(self.token)
        up.update_stockbase()
    
    def write_trade_data(self):
        stockbase_data=['ticker','secShortName','industryID1',
                        'industryID2','industryID3']
        #在tradedata中的字段
        stocktrade_data=['date','close']
        db=DatabaseInterface()
        tickers=db.get_db_tickers()
        for t in tickers:
            db.getone_db(self.db_name,'stockbase',{'ticker':t},
                         stockbase_data)
            
        
        
        
            
