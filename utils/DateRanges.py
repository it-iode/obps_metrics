# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 12:32:11 2020

@author: Administrator
"""

from datetime import datetime, timedelta
from collections import OrderedDict
import pandas as pd
import utils.db_connect as db_connect

def date_ranges(date_start, date_end):
    
    dates = [date_start, date_end]
    start, end = [datetime.strptime(_, "%Y-%m-%d") for _ in dates]
    date_list = OrderedDict(((start + timedelta(_)).strftime(r"%Y-%m"), None) for _ in xrange((end - start).days)).keys()
    
    dates_list = []
    for i in range(0,len(date_list)):
        dum = datetime.strptime(date_list[i], "%Y-%m")
        dum1 = datetime.strftime(dum,"%Y-%m-%d")
        dates_list.append(dum1)
        
    return dates_list

def date_ranges_month(date_start, date_end):
    
    dates = [date_start, date_end]
    start, end = [datetime.strptime(_, "%Y-%m-%d") for _ in dates]
    date_list = OrderedDict(((start + timedelta(_)).strftime(r"%Y-%m-%d"), None) for _ in xrange((end - start).days)).keys()
    
    dates_list = []
    for i in range(0,len(date_list)):
        dum = datetime.strptime(date_list[i], "%Y-%m-%d")
        dum1 = datetime.strftime(dum,"%Y-%m-%d")
        dates_list.append(dum1)
        
    return dates_list

def get_dates_from_db(table_name):
    # get dates already existing in table_name
    cursor, conn = db_connect.connect_db() 
    query = 'SELECT date_start FROM ' + table_name + ';'
    dates_query = db_connect.query_db(cursor, conn, query)
    dates_list = pd.DataFrame(dates_query, columns=['date_start'])  

    return dates_list

def check_db_dates(date_start, date_end):
    
    
    return