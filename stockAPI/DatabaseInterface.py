# -*- coding: utf-8 -*-
"""
Created on Mon Aug 03 14:09:45 2015

@author: saisn
"""
import pymongo
import stock_base as ba
import sys
import datetime
import errno
import time
from socket import error as socket_error


#数据库配置
#debug模式使用本地mongodb
#非debug模式使用mongolab在线数据库
DEBUG=False


if DEBUG:
    MONGODB_URI = "localhost"
    MONGODB_PORT=27017
    
else:
    MONGODB_URI ='mongodb://***:***@ds047642.mongolab.com:47642/stockdb'
    MONGODB_PORT=None

    
#数据库名称，本地、云一致
MONGODB_DB = "stockdb"


#数据库接口类
class DatabaseInterface(object):
    
    #数据库连接
    def __init__(self,MONGODB_URI=MONGODB_URI,MONGODB_PORT=MONGODB_PORT):
        #连接mongodb
        self.connection = pymongo.MongoClient(
                MONGODB_URI,MONGODB_PORT
            )
        self.db_name=MONGODB_DB
        
    
        
    #进入指定的collection
    #例子：
    #连接tempdb数据库的tempcol集合
    #dbin.connect_db('stockdb','stockbase')
    #参数说明：
    #db_name--mongo数据库名称，str
    #col_name--mongo集合名称，str 
    #返回：集合
    def connect_db(self,db_name,col_name):
        db = self.connection[db_name]
        collection = db[col_name]
        return collection
    
    #更新数据库信息
    def update_db(self,db_name,col_name,clause,updates,upsert=False, multi=False):
        collection=self.connect_db(db_name,col_name)
        return collection.update(clause,updates,upsert=upsert, multi=multi)
    
    #返回数据库的批量记录
    def get_db(self,db_name,col_name,clause,field):
        collection=self.connect_db(db_name,col_name)
        res=collection.find(clause,field)
        return res
        
    #返回数据库的一条记录
    def getone_db(self,db_name,col_name,clause,field):
        collection=self.connect_db(db_name,col_name)
        res=collection.find_one(clause,field)
        return res
    
    #清空一个集合
    def drop_db_docs(self,db_name,col_name):
        collection=self.connect_db(db_name,col_name)
        try:
            collection.remove({})
        except:
            print '文件清空失败！'
    
    #表的更新日期
    def update_db_date(self,db_name,col_name):
        date=col_name+'Date'
        update_col='currentinfo'
        clause={}
        updates={"$set":{date:self.today_as_str()}}
        self.update_db(db_name,update_col,clause,updates)

    #从currentinfo获取全部ticker列表
    def get_db_tickers(self):
        #数据库设置
        col_name='currentinfo'
        key='currentTickerList'
        #查询ticker列表
        clause={}
        field=[key]
        ticker_dict=self.getone_db(self.db_name,col_name,clause,field)
        return ticker_dict[key]
    
    
    #将读取的已格式化数据写入mongodb
    #考虑大量数据读写中断的情况
    def write_db_withlog(self,db_name,col_name,js):
        #db_name：数据库名称，str
        #col_name：collection名称，str
        #js：抓取的数据，已经格式化且提取了data，list of dic
        #返回：无
    

        
        
        update_step=col_name+'step'
        update_col='currentinfo'
        #进入指定的collection
        collection_write=self.connect_db(db_name,col_name)
        collection_step=self.connect_db(db_name,update_col)
        #显示进度用的计数器
        row_num=collection_step.find_one({},[update_step])[update_step]
        #将数据中的每一个字典插入到collection中作为一个文件
        for r in js:
            try:
                collection_write.insert(r)
            except socket_error as serr:
                print '连接出错，重试中......'
                time.sleep(5)

                
            
            row_num+=1
            collection_step.update({},{"$set":{update_step:row_num}})
            print str(row_num)+'row been written!'
        
        collection_step.update({},{"$set":{update_step:-1}})
    
    
    #将读取的已格式化数据写入mongodb
    def write_db(self,db_name,col_name,js):
        #db_name：数据库名称，str
        #col_name：collection名称，str
        #js：抓取的数据，已经格式化且提取了data，list of dic
        #返回：无
    
        #进入指定的collection
        collection_write=self.connect_db(db_name,col_name)
        row_num=0
        #将数据中的每一个字典插入到collection中作为一个文件
        for r in js:
            try:
                collection_write.insert(r)
            except:
                time.sleep(5)
                
            row_num+=1
            print str(row_num)+'row been written!'

    
    
    
    
    
    
    
    
    
