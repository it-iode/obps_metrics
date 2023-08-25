# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 08:14:37 2017

@author: cmunoz
"""
import config.Config as config

#import sys
#import re
import logging
import psycopg2

def connect_db():
    #reload(sys)  
    #sys.setdefaultencoding('utf8')

    settings = config.get_settings()
    conn = psycopg2.connect(dbname=settings['db_metrics_dbname'],host=settings['db_metrics_host'],
                            port=settings['db_metrics_port'], user=settings['db_metrics_username'], 
                            password=settings['db_metrics_password']) #define connection
    logging.info('Connecting to database\n	->%s' % (conn))
    cursor = conn.cursor()  # conn.cursor will return a cursor object, you can use this cursor to perform queries
    logging.info('Connected!\n')
    return cursor, conn;    

def query_db(cursor, conn, query):    
    cursor.execute(query)
    query_output = cursor.fetchall()
    cursor.close()
    conn.close()
    return query_output;

def delete_db(cursor, conn, query):    
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    return

def write_db(cursor, conn, query, arguments):
    cursor.execute(query,arguments)
    conn.commit()
    cursor.close()
    conn.close()
    return



    
    #    cursor.execute('SELECT organization.organization_name, remote_host.remote_host_ip  FROM analytics.remote_host, analytics.organization WHERE organization_id = remote_host_organization_id AND remote_host.remote_host_organization_id = 6;' + '\''+ qc_variable_name  + '\';')
#    varID = cursor.fetchall()