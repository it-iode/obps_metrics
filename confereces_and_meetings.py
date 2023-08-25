#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  4 15:23:52 2021

@author: a33272
"""

import traceback
import psycopg2
import pandas as pd
import config.Config as config
import utils.db_connect as db
import numpy as np

class ProcessConferences:
    def __init__(self):

        self.varnames_map = {
                                 'NAME': {'in_name': 'activity_name','out_name': 'activity_name'},
                                 'EVENT': {'in_name': 'event','out_name': 'event'},
                                 'TYPE': {'in_name': 'activity_type','out_name': 'activity_type'},
                                 'DATE': {'in_name': 'date','out_name': 'date'},
                                 'PUBLISHER': {'in_name': 'publisher','out_name': 'publisher'},
                                 'REF_PUB': {'in_name': 'reference_publication','out_name': 'reference_publication'},
                                 'COUNTRY': {'in_name': 'country','out_name': 'country'},
                                 'URL': {'in_name': 'drive_url','out_name': 'drive_url'},
                                }
        

        self.conferences_list_path = '/home/a33272/Downloads/conference_meetings_spreadsheet.xlsx'

    def load_conferences_list(self, file_path):
        df = pd.read_excel( file_path, sheet_name=0 )
        df = df.replace({np.nan:None})
        return df
    
    
if __name__ == '__main__':
    try:
        # Initialize related processing classes
        process = ProcessConferences() 
        settings = config.get_settings()
        
        df = process.load_conferences_list(process.conferences_list_path)
        
        df['date_short'] = ''
        for i in range(0,len(df)):
            date_short = str(df['date'].iloc[i])[0:7]
            df['date_short'].iloc[i] = date_short


        # # get existing entries codes from database
        # cursor, conn = db.connect_db()
        # query = '''SELECT activity_code FROM metrics.conferences;'''
        # query_out = db.query_db(cursor, conn, query)        
        # codes_list = [item for t in query_out for item in t]
        
        # Delete all conference registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.conferences;'''
        codes_list = db.delete_db(cursor, conn, query)
        
        #  Insert into database if activity_code does not exist        
        for i in range(len(df)) :
                    
            arguments = {'str0':df.loc[i, 'activity_code'],
                         'str1':df.loc[i, 'activity_name'], 
                         'str2':df.loc[i, 'event'],
                         'str3':df.loc[i, 'date'],
                         'str4':df.loc[i, 'activity_type'],
                         'str5':df.loc[i, 'drive_url'],    
                         'str6':df.loc[i, 'country'],
                         'str7':df.loc[i, 'publisher'],
                         }

            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.conferences (activity_code, activity_name, event, date, type, url, country, publisher) VALUES ( %(str0)s,%(str1)s, %(str2)s,%(str3)s,%(str4)s,%(str5)s,%(str6)s,%(str7)s);'''         
            db.write_db(cursor, conn, query, arguments)  
            print(str(df.loc[i, 'activity_code']) + '      written in database')            

            # if df.loc[i, 'activity_code'] not in codes_list:
            #     cursor, conn = db.connect_db()
            #     query = '''INSERT INTO metrics.conferences (activity_code, activity_name, event, date, type, url, country) VALUES ( %(str0)s,%(str1)s, %(str2)s,%(str3)s,%(str4)s,%(str5)s,%(str6)s);'''         
            #     db.write_db(cursor, conn, query, arguments)  
            #     print(str(df.loc[i, 'activity_code']) + '      written in database')
            # else:
            #     print(str(df.loc[i, 'activity_code']) + '  NOT written in database')

    

    except Exception:
        print(traceback.format_exc())    
