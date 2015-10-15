# -*- coding: utf-8 -*-
"""
Created on Thu Jul 23 15:16:24 2015

@author: saisn
"""
import os
os.chdir("C:\Users\saisn\Desktop\myfinance\stock data")
from StockMain import StockMain
from stockdata_setting import Client
from StockDailyUpdate import StockDailyUpdate


#db_name='stock'
#col_name='currentinfo'
#
#col_name='currentinfo'
token = '4648a6790ed5018390e3fe0d5d239bb4807cc762e645a08fe1bfba14dfc7ff70'
#ex=StockGetAll(token)
#db=DatabaseInterface()
#res=ex.get_all_tickers()
#currentinfo={}
#currentinfo['currentTickerList']=res
#currentinfo['currentTradeDate']=ex.today_as_str()
#db.write_db(db_name,col_name,[currentinfo])
#
##
#ex=StockDailyUpdate(token)
#ex.update_currentinfo()

task=StockMain()
#task=StockDailyUpdate(token)
#task.update_currentinfo()
#task.update_stockbase()
#写全部股票基本信息
#不是中断的情况，执行此语句会清除全部已有信息
#task.write_stockbase()

#写全部基金持股基本信息
#执行此语句会清除全部已有信息
#task.write_fundstockinfo(2013)

#写全部股票复权交易信息
#不是中断的情况，执行此语句会清除全部已有信息
beginDate='20130101'
task.write_stocktradeadj(beginDate,endDate='')




