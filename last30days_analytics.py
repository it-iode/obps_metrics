#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 08:18:22 2020

@author: Cristian Munoz Mas
"""
from datetime import datetime, timedelta
#from dateutil import relativedelta

import utils.HelloAnalyticsOBPS as analytics_obps
import utils.db_connect as db
import utils.DateRanges as DateRanges
import utils.AggregateInfo as AggregateInfo

def main():
    ## Last Month metrics
    date_start = datetime.strftime(datetime.now() - timedelta(30), '%Y-%m-%d')
    date_end = datetime.today().strftime('%Y-%m-%d')
    dates_list = DateRanges.date_ranges_month(date_start, date_end)
    
    cursor, conn = db.connect_db()
    query = '''DELETE FROM metrics.ganalytics_obpsorg_lastmonth;'''
    db.delete_db(cursor, conn, query)
    
    for i in range(0,len(dates_list)-1):
        
        dum_date_end = datetime.strptime(dates_list[i+1], '%Y-%m-%d')- timedelta(days=1)
        date_end = datetime.strftime(dum_date_end, '%Y-%m-%d')
        print 'Evaluating the period: ' + dates_list[i] + ' to ' + dates_list[i+1]
        total_new_users, total_users, total_countries, total_sessions, countries_df, pagepaths_df, sessions_user_df, users_df, countries_info, docs_info, response, start_date, end_date = analytics_obps.main(dates_list[i], date_end)
        docs_access = pagepaths_df['doc_path'].count()
        cursor, conn = db.connect_db()
        
        arguments = {'date1':start_date, 'date2':end_date, 'long3':long(total_new_users), 
                     'long4':long(total_users), 'long5':long(total_sessions), 
                     'long6':long(docs_access), 'long7':long(total_countries),
                     'json8':str(countries_info), 'json9':str(docs_info)}
        
        query = '''INSERT INTO metrics.ganalytics_obpsorg_lastmonth (date_start, date_end, users_num_new, users_num_total, visits_num, docs_access_num, countries_num, countries_info, docs_info) VALUES (%(date1)s, %(date2)s,%(long3)s,%(long4)s,%(long5)s,%(long6)s,%(long7)s,%(json8)s, %(json9)s);''' 
    
        db.write_db(cursor, conn, query, arguments)
    
    countries_info_df_agg_per_country = AggregateInfo.update_countries_info('lastmonth')
    docs_info_df_agg_per_doc = AggregateInfo.update_docs_info('lastmonth')


if __name__ == "__main__":
   main()
