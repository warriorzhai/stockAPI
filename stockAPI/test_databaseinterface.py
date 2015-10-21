# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 17:15:08 2015

@author: warriorzhai
"""


import os
os.chdir('C:/Users/warriorzhai/Desktop/project/stockAPI/stockAPI')
from DatabaseInterface import DatabaseInterface

#更换数据库修改以下三项
user_name='root'
psw='root'
MONGODB_URI ='mongodb://%s:%s@ds041394.mongolab.com:41394/tempdb'

MONGODB_URI=MONGODB_URI % (user_name,psw)
dbin=DatabaseInterface(MONGODB_URI=MONGODB_URI)
db_name='tempdb'
col_name='tempcol'

#测试connect_db
connection=dbin.connect_db(db_name,col_name)
connection.count()

#测试update_db
clause={"fund_name":"test"}
updates={"$set":{"fund_return":"2"}}
dbin.update_db(db_name,col_name,clause,updates,upsert=False, multi=False)




