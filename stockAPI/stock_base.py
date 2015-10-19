# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 17:07:56 2015

@author: warriorzhai
"""

import datetime



#获取当日日期的str格式
    #导入：datetime
    #例子：
    #输出今天日期 20150909
    #class.today_as_str()
    #输出今天日期 2015-09-09
    #class.today_as_str("%Y-%m-%d")
    #输出今天日期 09/09/2015
    #class.today_as_str("%m/%d/%Y")
    #返回：当日字符串
    #参数说明：
    #date_format--日期的输出格式，字符串
def today_as_str(date_format="%Y%m%d"):
    
    #获取当前日期datetime.datetime格式
    now = datetime.datetime.now()
    #转化为string格式
    todate=now.strftime(date_format)
    return todate