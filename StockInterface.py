# -*- coding: utf-8 -*-
"""
Created on Mon Aug 03 14:11:05 2015

@author: saisn
"""

import csv
import json
import sys
import datetime
import pymongo
import time
import pandas as pd
import tushare as ts
import os
os.chdir("C:\Users\saisn\Desktop\myfinance\stock data")
from stockdata_setting import Client

class StockInterface(object):
    
    def __init__(self,token):
        self.token=token
    
    #将抓取的数据json格式化
    def json_format(self,js):
        #js：抓取的源数据
        #返回：格式化后的数据，list of dic
        js=unicode(js,errors='ignore')
        js=json.loads(js)
        return js
    
    def _getdata(self,url):
        while True:
        #连接接口
            client = Client()
            client.init(self.token)
            #获取数据
            
            try:
                code, result = client.getData(url)
            except:
                print '连接出错，重试中......'
                time.sleep(5)
                continue
            break
        
        if code==200:
            #数据获取成功，json格式化
            res=self.json_format(result)
            if 'data' not in res:
                print '没有找到数据'
                res['data']=None
            return res['data']
        else:
            #数据获取失败，强制结束
            print code
            sys.exit()
    
#        #接口函数，获取全部股票基本信息
#    def _getSecID(self,assetClass='E'):
#        #token：开发用接口权限码，str
#        #assetClass：获取的数据类型（股票、债券、期货...），str
#        #返回：成功返回json格式化的数据，不成功程序结束
#    
#        #接口抓取字段配置
#        fields=['secID','ticker','secShortName','exchangeCD',\
#        'partyID','listStatusCD']
#        outputs=','.join(fields)
#        #接口地址配置
#        url='/api/master/getSecID.json?field=%s&assetClass=%s&ticker=&partyID=&cnSpell='
#        url=url % (outputs,assetClass)
#        return self._getdata(url)
#    
    
    #接口函数，获取全部股票基本信息
    def _getEqu(self,fields,ticker='',listStatusCD='L',equTypeCD='A'):
        #token：开发用接口权限码，str
        #assetClass：获取的数据类型（股票、债券、期货...），str
        #返回：成功返回json格式化的数据，不成功程序结束
    
        #接口抓取字段配置
        outputs=','.join(fields)
        #处理抓取多个股票ticker的情况
        if type(ticker)==list:
            ticker=','.join(ticker)
        #接口地址配置
        url='/api/equity/getEqu.json?field=%s&listStatusCD=%s&secID=&ticker=%s&equTypeCD=%s'
        url=url % (outputs,listStatusCD,ticker,equTypeCD)
        return self._getdata(url)
        
        
    
    #接口函数，获取股票代码=ticker，时间beginDate~endDate的股票的历史交易数据
    #可选择抓取的字段
    def _getMktEqud(self,ticker,fields,beginDate,endDate,tradeDate):
        #接口抓取字段配置
        outputs=','.join(fields)
        #处理抓取多个股票ticker的情况
        if type(ticker)==list:
            ticker=','.join(ticker)
        #接口地址配置
        url='/api/market/getMktEqud.json?field=%s&beginDate=%s&endDate=%s&secID=&ticker=%s&tradeDate=%s'
        url=url % (outputs,beginDate,endDate,ticker,tradeDate)
        return self._getdata(url)
    
    
    
    #大宗交易数据
    def _getMktBlockd(self,ticker,fields,beginDate='',endDate='',tradeDate='',assetClass='E'):
        #接口抓取字段配置
        outputs=','.join(fields)
        #处理抓取多个股票ticker的情况
        if type(ticker)==list:
            ticker=','.join(ticker)
        #接口地址配置
        url='/api/market/getMktBlockd.json?field=%s&beginDate=%s&endDate=%s&secID=&ticker=%s&assetClass=%s&tradeDate=%s'
        url=url % (outputs,beginDate,endDate,ticker,assetClass,tradeDate)
        return self._getdata(url)
            
    #配股信息
    def _getEquAllot(self,ticker,fields,beginDate,endDate):
        outputs=','.join(fields)
        #处理抓取多个股票ticker的情况
        if type(ticker)==list:
            ticker=','.join(ticker)
        #接口地址配置
        url='/api/equity/getEquAllot.json?field=%s&ticker=%s&secID=&beginDate=%s&endDate=%s&isAllotment=1'
        url=url % (outputs,ticker,beginDate,endDate)
        return self._getdata(url)
    
    #分红信息
    def _getEquDiv(self,ticker,fields,beginDate,endDate):
        outputs=','.join(fields)
        #处理抓取多个股票ticker的情况
        if type(ticker)==list:
            ticker=','.join(ticker)
        #接口地址配置
        url='/api/equity/getEquDiv.json?field=%s&ticker=%s&secID=&beginDate=%s&endDate=%s&eventProcessCD=6'
        url=url % (outputs,ticker,beginDate,endDate)
        return self._getdata(url)
            
    #行业信息     
    def _getEquIndustry(self,ticker,fields):    
        #接口抓取字段配置
        outputs=','.join(fields)
        #处理抓取多个股票ticker的情况
        if type(ticker)==list:
            ticker=','.join(ticker)
        industryVersionCD='010303'
        now = datetime.datetime.now()
        todate=now.strftime("%Y%m%d")
        #接口地址配置
        url='/api/equity/getEquIndustry.json?field=%s&industryVersionCD=%s&industry=&secID=&ticker=%s&intoDate=%s'
        url=url % (outputs,industryVersionCD,ticker,todate)
        return self._getdata(url)
    
    #今日停牌复牌信息
    def _getSecTips(self,fields,tips):
        #接口抓取字段配置
        outputs=','.join(fields)
        #接口地址配置
        url='/api/market/getSecTips.json?field=%s&tipsTypeCD=%s'
        url=url % (outputs,tips)
        return self._getdata(url)
    
    #复权因子
    def _getMktAdjf(self,fields,ticker):
        #接口抓取字段配置
        outputs=','.join(fields)
        #处理抓取多个股票ticker的情况
        if type(ticker)==list:
            ticker=','.join(ticker)
        #接口地址配置
        url='/api/market/getMktAdjf.json?field=%s&secID=&ticker=%s'
        url=url % (outputs,ticker)
        return self._getdata(url)
        
    #最近一次合并利润表
    def _getFdmtISLately(self,fields):
        #接口抓取字段配置
        outputs=','.join(fields)
        #接口地址配置
        url='/api/fundamental/getFdmtISLately.json?field=%s'
        url=url % (outputs)
        return self._getdata(url)
        
    #==========TS 接口==================
    #复权交易数据
    #返回字典列表格式
    def _getTradeDataAdj(self,ticker,beginDate='',endDate=''):
        #日期格式化，tushare接口日期格式为'%Y-%m-%d'
        date_format=lambda x:datetime.datetime.strptime(x, '%Y%m%d').strftime('%Y-%m-%d')
        beginDate,endDate=[date_format(date) if date else date for date in [beginDate,endDate]]
        #获取数据
        while True:
            try:
                res=ts.get_h_data(ticker,start=beginDate,end=endDate)
            except:
                print '连接出错，重试中......'
                time.sleep(5)
                continue
            break
        
        if res is None:
            return None
        else:
            #加上日期列
            #日期转为string格式
            res['date']=res.index.format()
            return json.loads(res.T.to_json()).values()
    
    #基金季度持股信息
    #返回dataframe格式
    def _getFundStocksData(self,year,quater):
        try:
            res = ts.fund_holdings(year,quater)
        except IOError as e:
            print str(year)+' '+str(quater)+'季度没有数据。'
            return None
        print str(year)+' '+str(quater)+'季度数据读取成功！'
        return res
    
    
    #上市公司利润分配预案,转股、分红
    #返回dataframe格式
    def _getRevenuData(self,year,top=100):
        res = ts.profit_data(year=year,top=top)
        return res
    
    
    
    
    
    
    
        