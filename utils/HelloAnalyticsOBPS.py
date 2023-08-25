# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 15:34:24 2020

@author: Administrator
"""


import sys
sys.path.append("..") # Adds higher directory to python modules path.



import config.Config as config
#from googleapiclient import discovery
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import json
import pandas as pd


settings = config.get_settings()
SCOPES = settings['ga_scopes']
KEY_FILE_LOCATION = settings['ga_key_file_location_searchengine']
VIEW_ID = settings['ga_view_id_searchengine']

def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


def get_report(analytics, start_date, end_date):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'pageSize': 100000,
          'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
          'metrics': [{'expression': 'ga:sessions'},
                      {'expression': 'ga:users'},
                      {'expression': 'ga:newUsers'},
                      {'expression':'ga:sessionsPerUser'}],
          'dimensions': [{'name': 'ga:country'}, {'name': 'ga:pagePath'}]
          #'dimensions': [{'name': 'ga:country'}]
        }]
      }
  ).execute()

def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response.

  Args:
    response: An Analytics Reporting API V4 response.
  """
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    country_list = []
    user_list = []
    new_user_list = []
    session_list = []
    #pageviews_list = []
    pagepaths_list = []
    sessions_user_list = []

    columns = ['country','sessions','users']
    countries_df = pd.DataFrame(columns = columns)

    columns = ['doc_path']
    pagepaths_df = pd.DataFrame(columns = columns)

    columns = ['sessions_user']
    sessions_user_df = pd.DataFrame(columns = columns)

    columns = ['users', 'new_users']
    users_df = pd.DataFrame(columns = columns)


    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])

      for header, dimension in zip(dimensionHeaders, dimensions):
        #print(header + ': ' + dimension)

        if header == 'ga:country':
            #country_list.append(dimension)
            country = dimension
        elif header == 'ga:pagePath':
            #pagepaths_list.append(dimension)
            doc_path = dimension
            #print(str(pagepath))
        for i, values in enumerate(dateRangeValues):
        #print('Date range: ' + str(i))
            for metricHeader, value in zip(metricHeaders, values.get('values')):
              #print(metricHeader.get('name') + ': ' + value)
              if metricHeader.get('name') == 'ga:users':
                  #user_list.append(value)
                  user = value
              elif metricHeader.get('name') == 'ga:newUsers':
                  #new_user_list.append(value)
                  new_user = value
              elif metricHeader.get('name') == 'ga:sessions':
                  #session_list.append(value)
                  session = value
              # elif metricHeader.get('name') == 'ga:sessionsPerUser':
              #     #sessions_user_list.append(value)
              #     sessions_user = value
      new_row = pd.Series({'users':int(user), 'new_users': int(new_user)})
      users_df = pd.concat([users_df, new_row.to_frame().T], ignore_index=True)
      #users_df = users_df.append({'users':int(user), 'new_users': int(new_user)}, ignore_index=True)
      new_row = pd.Series({'country':country, 'users':int(user),'sessions': int(session)})
      countries_df = pd.concat([countries_df, new_row.to_frame().T], ignore_index=True)
      #countries_df = countries_df.append({'country':country, 'users':int(user),'sessions': int(session)}, ignore_index=True)
      countries_df = countries_df[~countries_df.country.str.contains("Cayman Islands")]
      countries_df = countries_df[~countries_df.country.str.contains("(not set)")]
      #sessions_user_df = sessions_user_df.append({'sessions_user': sessions_user}, ignore_index=True)
      new_row = pd.Series({'country':country, 'sessions': session, 'users': user, 'doc_path':doc_path})
      pagepaths_df = pd.concat([pagepaths_df, new_row.to_frame().T], ignore_index=True)
      #pagepaths_df = pagepaths_df.append({'country':country, 'sessions': session, 'users': user, 'doc_path':doc_path}, ignore_index=True)
      # countries_df = countries_df.append({'country':country, 'sessions': session, 'users': user}, ignore_index=True)
      # pagepaths_df = pagepaths_df.append({'country':pagepath, 'sessions': session, 'users': user, 'pagepaths':pagepath}, ignore_index=True)
  users_df.users = users_df.users.astype(int)
  users_df.new_users = users_df.new_users.astype(int)
  pagepaths_df.users = pagepaths_df.users.astype(int)
  pagepaths_df.sessions = pagepaths_df.sessions.astype(int)
  pagepaths_df = pagepaths_df[pagepaths_df.doc_path.str.contains("/handle/")]
  pagepaths_df = pagepaths_df[pagepaths_df.doc_path.str.contains(".pdf")]
  pagepaths_df = pagepaths_df[~pagepaths_df.doc_path.str.contains(".jpg")]
  pagepaths_df = pagepaths_df.sort_values(['sessions'], ascending=False)
  pagepaths_df = pagepaths_df.groupby('doc_path').agg({'country':'count', 'sessions': 'sum', 'users': 'sum'}).reset_index().rename(columns={'country':'countries'})
  docs_info = pagepaths_df.to_json(orient='table')
  countries_df = countries_df.groupby('country').agg({'sessions': 'sum', 'users': 'sum'}).reset_index()
  countries_info = countries_df.to_json(orient='table')
  # countries_df.users = countries_df.users.astype(int)
  # countries_df.sessions = countries_df.sessions.astype(int)
  # #countries_df = countries_df.sort_values(['sessions'], ascending=False)
  # # pagepaths_df.sessions = pagepaths_df.sessions.astype(int)
  # # pagepaths_df = pagepaths_df.groupby(['pagepaths']).count()
  # # pagepaths_df = pagepaths_df[pagepaths_df.index.str.contains("/handle/")]
  # # pagepaths_df = pagepaths_df[~pagepaths_df.index.str.contains(".jpg")]
  # # pagepaths_df = pagepaths_df.sort_values(['sessions'], ascending=False)
  # countries_df = countries_df.groupby('country', as_index=False).sum()
  # print(countries_df.country)
  # countries_df = countries_df.sort_values(['sessions'], ascending=False)
  # #countries_df = countries_df.head(10)
  # #pagepaths_df = pagepaths_df.head(10)
  # #print(pagepaths_df)
  # #countries_df = countries_df.reset_index(drop=True)
  return new_user_list, user_list, country_list, session_list, countries_df,pagepaths_df, sessions_user_df, users_df, countries_info, docs_info

def main(start_date, end_date):
  analytics = initialize_analyticsreporting()
  response = get_report(analytics, start_date, end_date)
  new_user_list, user_list, country_list, session_list, countries_df,pagepaths_df, sessions_user_df, users_df, countries_info, docs_info = print_response(response)
  # for i in range(0, len(new_user_list)):
  #     new_user_list[i] = int(new_user_list[i])
  # for i in range(0, len(user_list)):
  #     user_list[i] = int(user_list[i])
  # for i in range(0, len(session_list)):
  #     session_list[i] = int(session_list[i])

  total_users = users_df.users.sum()
  total_new_users = users_df.new_users.sum()
  total_countries = countries_df.shape[0]
  total_sessions = countries_df.sessions.sum()

  return total_new_users, total_users, total_countries, total_sessions, countries_df, pagepaths_df, sessions_user_df, users_df, countries_info, docs_info, response, start_date, end_date

