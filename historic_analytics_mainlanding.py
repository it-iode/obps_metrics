# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 15:45:11 2020

@author: Cristian Munoz Mas

Collects metrics from the obpsystem.org application in Google Analytics. 
"""
from datetime import datetime, timedelta
from dateutil import relativedelta

from . import utils.HelloAnalyticsOBPS_mainlanding as analytics_obps_mainlanding
from . import utils.db_connect as db
from . import utils.DateRanges as DateRanges
from . import utils.AggregateInfo as AggregateInfo

def main(date_start, date_end):
    #set list of dates to process
    date_start = '2023-06-01'
    date_end = '2023-07-01'
    dates_list = DateRanges.date_ranges(date_start, date_end)
    if len(dates_list) == 1:
        dates_list.append(date_end)
        
    #check already existing dates into db
    table_name = 'metrics.ganalytics_obpsystem'
    dates_df_db = DateRanges.get_dates_from_db(table_name)
    dates_list_db = []
    for dat in dates_df_db.date_start:
        dates_list_db.append(dat.strftime('%Y-%m-%d'))
        
    for i in range(0,len(dates_list)-1):
        
        dum_date_end = datetime.strptime(dates_list[i+1], '%Y-%m-%d')- timedelta(days=1)
        date_end = datetime.strftime(dum_date_end, '%Y-%m-%d')
        print('Evaluating the period: ' + dates_list[i] + ' to ' + dates_list[i+1])
        total_new_users, total_users, total_countries, total_sessions, countries_df, sessions_user_df, users_df, countries_info, response, start_date, end_date = analytics_obps_mainlanding.main(dates_list[i], date_end)
        cursor, conn = db.connect_db()
        
        arguments = {'date1':start_date, 'date2':end_date, 'long3':int(total_new_users), 
                     'long4':int(total_users), 'long5':int(total_sessions), 
                     'long7':int(total_countries), 'json8':str(countries_info)
                     }
        
        if dates_list[i] in dates_list_db:
            print('The period ' + dates_list[i] + ' to ' + dates_list[i+1] + ' alrady exists. Results will be overwritten for this period.')
            query = 'UPDATE metrics.ganalytics_obpsystem SET date_start = %(date1)s, date_end = %(date2)s, users_num_new = %(long3)s, users_num_total = %(long4)s, visits_num = %(long5)s, countries_num = %(long7)s, countries_info = %(json8)s WHERE date_start  = \'' + dates_list[i] + '\';'       
        elif dates_list[i] not in dates_list_db:
            query = '''INSERT INTO metrics.ganalytics_obpsystem (date_start, date_end, users_num_new, users_num_total, visits_num, countries_num, countries_info) VALUES (%(date1)s, %(date2)s,%(long3)s,%(long4)s,%(long5)s,%(long7)s,%(json8)s);''' 
    
        db.write_db(cursor, conn, query, arguments)
        
    countries_info_df_agg_per_country = AggregateInfo.update_countries_info('historic_mainlanding')
    #docs_info_df_agg_per_doc = AggregateInfo.update_docs_info('historic')    


if __name__ == "__main__":    
    date_end = datetime.today().replace(day=1)
    date_start = date_end - timedelta(days=1)
    date_start = date_start.replace(day=1)
    date_end = date_end.strftime('%Y-%m-%d')
    date_start = date_start.strftime('%Y-%m-%d')
    main(date_start, date_end)
    #main(sys.argv[0], sys.argv[1])

