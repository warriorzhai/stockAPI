# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 17:15:08 2015

@author: warriorzhai
"""

from DatabaseInterface import DatabaseInterface
import os
os.chdir('C:\Users\warriorzhai\Desktop\project\stockAPI\stockAPI')

#更换数据库修改以下三项
user_name='root'
psw='shinan1988'
MONGODB_URI ='mongodb://%s:%s@ds041394.mongolab.com:41394/tempdb'

MONGODB_URI=MONGODB_URI % (user_name,psw)
dbin=DatabaseInterface(MONGODB_URI=MONGODB_URI)


#测试connect_db
db_name='tempdb'
col_name='tempcol'
connection=dbin.connect_db(db_name,col_name)
connection.count()


