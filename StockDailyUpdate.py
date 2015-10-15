# -*- coding: utf-8 -*-
"""
Created on Mon Aug 03 11:28:43 2015

@author: saisn
"""
from stockdata_setting import Client
from DatabaseInterface import DatabaseInterface
from StockGetAll import StockGetAll
from StockInterface import StockInterface
import csv
import json
import sys
import os
import datetime
import pymongo
import time
os.chdir("C:\Users\saisn\Desktop\myfinance\stock data")



class StockDailyUpdate(object):
    def __init__(self,token):
        self.token=token
        self.stkif=StockInterface(token)
        self.db_name='stock'
    
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
    
    #找出新增的ticker
    def get_newticker(self):
        col_name='stockbase'
        key='ticker'
        db=DatabaseInterface()
        tickers_new=db.get_db_tickers()
        print 'getting new tickers with length:'+str(len(tickers_new))+'....'
        clause={}
        field=[key]
        stockbase_data=db.get_db(self.db_name,col_name,clause,field)
        tickers_old=self.unpack_dic(key,stockbase_data)
        print 'getting old tickers with length:'+str(len(tickers_old))+'....'
        tickers_addition=list(set(tickers_new)-set(tickers_old))
        return tickers_addition
        
    #将新增的ticker信息写入
    def update_new_ticker(self):
        col_name='stockbase'
        stlget=StockGetAll(self.token)
        db=DatabaseInterface()
        #获取新增的ticker
        new_tickers=self.get_newticker()
        #写入新ticker的信息
        stocks_gene=stlget.get_api_stockbase(new_tickers)
        db.write_db(self.db_name,col_name,stocks_gene)
    
    def update_stockbase_Equ(self,fields):
        col_name='stockbase'
        stkif=StockInterface(self.token)
        db=DatabaseInterface()
        tickers=db.get_db_tickers()
        for t in tickers:
            #获取字典
            Equ_dics=stkif._getEqu(fields,t)[0]
            db.update_db(self.db_name,col_name,{"ticker":t},{"$set":Equ_dics})
            print 'update stockbase data with ticker.no'+t+'....'

    def update_stockbase_SecTips(self,tips):
        col_name='stockbase'
        fields='ticker'
        stkif=StockInterface(self.token)
        db=DatabaseInterface()
        tickers_dic=stkif._getSecTips([fields],tips)
        tickers=self.unpack_dic(fields,tickers_dic)
        updates={"$set":{'tipsTypeCD':tips}}
        for t in tickers:
            print t+'today turn to'+tips
            db.update_db(self.db_name,col_name,{'ticker':t},updates)
        
    
    #更新
    def update_currentinfo(self):
        col_name='currentinfo'
        db=DatabaseInterface()
        stlget=StockGetAll(self.token)
        res=stlget.get_api_tickers()
        clause={}
        updates={'$set':{'currentTickerList':res}}
        db.update_db(self.db_name,col_name,clause,updates)
        db.update_db_date(self.db_name,'ticker')
    
    def update_stockbase(self):
        col_name='stockbase'
        db=DatabaseInterface()
        #将新增的ticker信息写入
        self.update_new_ticker()
        print 'new tickers update!'
        #更新stockbase数据
        fields=["nonrestFloatShares","totalShares" ,"nonrestfloatA"]
        #更新stockbase数据：更新Equ接口中字段
        self.update_stockbase_Equ(fields)
        print 'shares data update!'
        #更新stockbase数据：更新停牌、复牌状态
        self.update_stockbase_SecTips('H')
        print 'H state update!'
        self.update_stockbase_SecTips('R')
        print 'R state update!'
        db.update_db_date(self.db_name,col_name)
        print 'stockbase update finished....'

    #每日更新股票交易数据
    #后期改善成更新所有的未更新的数据
    def update_ticker_trade(token):
        connection = pymongo.MongoClient(
                MONGODB_URI,MONGODB_PORT
            )
        db = connection[MONGODB_DB]
        collection = db['stocktrade']
        now = datetime.datetime.now()
        todate=now.strftime("%Y%m%d")
        stock_fields=['ticker','tradeDate','closePrice','negMarketValue','MarketValue','turnoverVol','turnoverValue','dealAmount','PE']
        res=_getMktEqud(token,'',stock_fields,'','',todate)
        if res:
            for r in res:
                t=str(r.pop('ticker'))
                collection.update({"ticker":t},{'$push':{'data':r}})
                print "one row inserted!"
        else:
            print "系统还没有更新出今日"+todate+"的数据！"
    
            
            
            
            
            
            