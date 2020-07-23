# -*- coding: utf-8 -*-
"""
Created on Wed Feb 19 11:38:02 2020

@author: Cristian Munoz Mas
"""
import utils.db_connect as db

import pandas as pd

def convert_json_to_df(json_data):
    data_df = pd.DataFrame.from_dict(json_data['data'])   
    return data_df

def get_number_rows_db():
    cursor, conn = db.connect_db()
    query = 'SELECT COUNT (*) FROM metrics.ganalytics_obpsorg;'
    output = db.query_db(cursor, conn, query)
    num_rows = int(output[0][0])
    return num_rows

#def get_countries_info_db(num_rows):
#    cursor, conn = db.connect_db()
#    #query = 'SELECT ganalytics_obpsorg.countries_info FROM metrics.ganalytics_obpsorg LIMIT ' + str(row_num) + ' OFFSET ' +  str(row_num-1) + ';'
#    query = 'SELECT ganalytics_obpsorg.countries_info FROM metrics.ganalytics_obpsorg LIMIT ' + str(num_rows) + ';'
#    countries_info_json = db.query_db(cursor, conn, query) 
#    return countries_info_json

def get_attr_info_db(num_rows, attr, mode):
    cursor, conn = db.connect_db()
    if attr == 'countries':
        if mode == 'historic_mainlanding':
            table = 'ganalytics_obpsystem'
            column = table + '.countries_info'
        if mode == 'historic':
            table = 'ganalytics_obpsorg'
            column = table + '.countries_info'
        elif mode == 'lastmonth':
            table = 'ganalytics_obpsorg_lastmonth'
            column = table + '.countries_info'
    elif attr == 'docs':
        if mode == 'historic':
            table = 'ganalytics_obpsorg'
            column = table + '.docs_info'
        elif mode == 'lastmonth':
            table = 'ganalytics_obpsorg_lastmonth'
            column = table + '.docs_info'
    query = 'SELECT ' + column +  ' FROM metrics.' + table + ' LIMIT ' + str(num_rows) + ';'  
    attr_info_json = db.query_db(cursor, conn, query) 
    return attr_info_json
    
def update_countries_info(mode):
    if mode == 'historic_mainlanding':
        table_name = 'metrics.ganalytics_obpsystem_countries'
    elif mode == 'historic':
        table_name = 'metrics.ganalytics_obpsorg_countries'
    elif mode == 'lastmonth':
        table_name = 'metrics.ganalytics_obpsorg_lastmonth_countries'
        
    num_rows = get_number_rows_db()
    columns = ['country', 'sessions', 'users']
    countries_info_df_agg = pd.DataFrame(columns = columns)
    countries_info_json = get_attr_info_db(num_rows, 'countries', mode)
    for i in range(0,len(countries_info_json)):        
        countries_info_df = convert_json_to_df(countries_info_json[i][0])
        #countries_info_df = countries_info_df.reset_index()
        countries_info_df = countries_info_df.drop(columns='index')
        countries_info_df_agg = countries_info_df_agg.append(countries_info_df, ignore_index=True)
        countries_info_df_agg_per_country = countries_info_df_agg.groupby(['country']).sum().reset_index()
        
    cursor, conn = db.connect_db()
    query = '''DELETE FROM ''' + table_name + ';'
    db.delete_db(cursor, conn, query)
    for i in range(0,len(countries_info_df_agg_per_country)):
        country_name = countries_info_df_agg_per_country.country.iloc[i].encode('utf-8')
        users_num = countries_info_df_agg_per_country.users.iloc[i]
        sessions_num = str(countries_info_df_agg_per_country.sessions.iloc[i])
        cursor, conn = db.connect_db()
        arguments = {'str1':country_name, 'long2':long(users_num), 'long3':long(sessions_num)}    
        query = 'INSERT INTO ' + table_name + ' (country, users, sessions) VALUES (%(str1)s, %(long2)s,%(long3)s);' 
        db.write_db(cursor, conn, query, arguments)
    return countries_info_df_agg_per_country

def update_docs_info(mode):
    if mode == 'historic':
        table_name = 'metrics.ganalytics_obpsorg_docs'
    elif mode == 'lastmonth':
        table_name = 'metrics.ganalytics_obpsorg_lastmonth_docs'
    num_rows = get_number_rows_db()
    columns = ['doc_path', 'countries', 'sessions', 'users']
    docs_info_df_agg = pd.DataFrame(columns = columns)
    docs_info_json = get_attr_info_db(num_rows, 'docs', mode)
    for i in range(0,len(docs_info_json)):        
        docs_info_df = convert_json_to_df(docs_info_json[i][0])
        docs_info_df = docs_info_df.drop(columns='index')
        docs_info_df_agg = docs_info_df_agg.append(docs_info_df, ignore_index=True)
        docs_info_df_agg_per_doc = docs_info_df_agg.groupby(['doc_path']).sum().reset_index()
    cursor, conn = db.connect_db()
    query = '''DELETE FROM ''' + table_name + ';'
    db.delete_db(cursor, conn, query)
    for i in range(0,len(docs_info_df_agg_per_doc)):
        doc_path = docs_info_df_agg_per_doc.doc_path.iloc[i].encode('utf-8')
        countries_num = docs_info_df_agg_per_doc.countries.iloc[i]
        users_num = docs_info_df_agg_per_doc.users.iloc[i]
        sessions_num = str(docs_info_df_agg_per_doc.sessions.iloc[i])
        cursor, conn = db.connect_db()
        arguments = {'long1':long(countries_num), 'long2':long(users_num), 'long3':long(sessions_num), 'str4': doc_path}    
        query = 'INSERT INTO ' + table_name + ' (doc_path, countries, users, sessions) VALUES (%(str4)s, %(long1)s, %(long2)s, %(long3)s);' 
        db.write_db(cursor, conn, query, arguments)
    return docs_info_df_agg_per_doc