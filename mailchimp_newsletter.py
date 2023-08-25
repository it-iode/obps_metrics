#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  4 15:23:52 2021

@author: a33272
"""

import traceback
import psycopg2
import pandas as pd
from . import config.Config as config
from . import utils.db_connect as db
import numpy as np
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError

class ProcessNewsletter:
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
        

    def _get_all_campaign_reports(self):
        """Return a list of dictionaries containing reports. """
        try:
            client = MailchimpMarketing.Client()
            client.set_config({
                "api_key": API_KEY,
                "server": SERVER_PREFIX
            })
    
            response = client.reports.get_all_campaign_reports(count=1000)
            return response
        except ApiClientError as error:
              print("Error: {}".format(error.text))
    
    def reports_to_pandas(self):
        """Convert a Mailchimp reports dictionary to a Pandas dataframe."""
        
        reports = self._get_all_campaign_reports()
        
        df = pd.DataFrame(columns=['id', 'send_time','campaign_title', 'type', 'list_name', 'subject_line', 
                                   'preview_text', 'emails_sent', 'abuse_reports', 'unsubscribed', 
                                   'hard_bounces', 'soft_bounces', 'syntax_errors', 'forwards_count',
                                   'forwards_opens', 'opens_total', 'unique_opens', 'open_rate',
                                   'clicks_total', 'unique_clicks', 'unique_subscriber_clicks', 
                                   'click_rate', 'list_sub_rate', 'list_unsub_rate', 'list_open_rate', 
                                   'list_click_rate', 'total_orders', 'total_revenue'
                                  ])
        
        if reports:
            for report in reports['reports']: 
                row = {
                    'id': report.get('id'),
                    'send_time': report.get('send_time'),                
                    'campaign_title': report.get('campaign_title'),
                    'type': report.get('type'),
                    'list_name': report.get('list_name'),
                    'subject_line': report.get('subject_line'),
                    'preview_text': report.get('preview_text'),
                    'emails_sent': report.get('emails_sent'),
                    'abuse_reports': report.get('abuse_reports'),
                    'unsubscribed': report.get('unsubscribed'),
                    'hard_bounces': report.get('bounces').get('hard_bounces'),
                    'soft_bounces': report.get('bounces').get('soft_bounces'),
                    'syntax_errors': report.get('syntax_errors'),
                    'forwards_count': report.get('forwards').get('forwards_count'),
                    'forwards_opens': report.get('forwards').get('forwards_opens'),
                    'opens_total': report.get('opens').get('opens_total'),
                    'unique_opens': report.get('opens').get('unique_opens'),
                    'open_rate': report.get('opens').get('open_rate'),
                    'clicks_total': report.get('clicks').get('clicks_total'),
                    'unique_clicks': report.get('clicks').get('unique_clicks'),
                    'unique_subscriber_clicks': report.get('clicks').get('unique_subscriber_clicks'),
                    'click_rate': report.get('clicks').get('click_rate'),
                    'list_sub_rate': report.get('list_stats').get('sub_rate'),
                    'list_unsub_rate': report.get('list_stats').get('unsub_rate'),
                    'list_open_rate': report.get('list_stats').get('open_rate'),
                    'list_click_rate': report.get('list_stats').get('click_rate'),
                    'total_orders': report.get('ecommerce').get('total_orders'),
                    'total_revenue': report.get('ecommerce').get('total_revenue'),
                }
                
                df = df.append(row, ignore_index='True')
            
            df = df.fillna(0)
                
            for col in ['emails_sent', 'abuse_reports', 'unsubscribed', 
                        'hard_bounces', 'soft_bounces', 'syntax_errors',
                        'forwards_count', 'forwards_opens', 'opens_total', 
                        'unique_opens', 'open_rate', 'clicks_total', 
                        'unique_clicks', 'unique_subscriber_clicks', 'click_rate',
                        'list_sub_rate', 'list_unsub_rate', 'list_unsub_rate', 
                        'list_open_rate', 'list_click_rate', 'total_orders']:
                df[col] = df[col].astype(int)
                
            df['total_revenue'] = df['total_revenue'].astype(float)
            
            return df, reports    
        
        
    def _get_growth_history(self, list_id):
        """Measure the growth history of a mailing list."""
        try:
            client = MailchimpMarketing.Client()
            client.set_config({
                "api_key": API_KEY,
                "server": SERVER_PREFIX
            })
            
            response = client.lists.get_list_growth_history(list_id, count=1000)
            return response
        except ApiClientError as error:
              print("Error: {}".format(error.text))       
        
        
    def growth_history_to_pandas(self, list_id):
        """Convert a Mailchimp growth history dictionary to a Pandas dataframe."""
        
        results = self._get_growth_history(list_id)
    
        df = pd.DataFrame(columns=['month', 'subscribed'])
        
        if results:
            for data in results['history']: 
                row = {
                    'month': data.get('month'),
                    'subscribed': data.get('subscribed'),
                }
            
                df = df.append(row, ignore_index='True')
            
            for col in ['subscribed']:
                df[col] = df[col].astype(int)
            
            df['list_change_mom'] = df['subscribed'] - df['subscribed'].shift(-1)
            df['list_change_yoy'] = df['subscribed'] - df['subscribed'].shift(-12)
            return df       
        



    def _get_locations(self, list_id):
        """Get locations of a mailing list."""
        try:
            client = MailchimpMarketing.Client()
            client.set_config({
                "api_key": API_KEY,
                "server": SERVER_PREFIX
            })
            
            response = client.lists.get_list_locations(list_id)
            return response
        except ApiClientError as error:
              print("Error: {}".format(error.text)) 



    def locations_to_pandas(self, list_id):
        """Convert a Mailchimp list locations to a Pandas dataframe."""
        
        results = self._get_locations(list_id)
    
        df = pd.DataFrame(columns=['country', 'cc', 'percent', 'total'])
        
        if results:
            for data in results['locations']: 
                row = {
                    'country': data.get('country'),
                    'cc': data.get('cc'),
                    'percent': data.get('percent'),
                    'total': data.get('total'),
                }
            
                df = df.append(row, ignore_index='True')
                df['total'] = df['total'].astype(int)
            
            return df          
        
        
        
        
if __name__ == '__main__':
    try:
        # Initialize related processing classes
        process = ProcessNewsletter() 
        settings = config.get_settings()
        
        API_KEY = settings['mailchimp_api_key']
        SERVER_PREFIX = 'us13'

        client = MailchimpMarketing.Client()
        client.set_config({
            "api_key": API_KEY,
            "server": SERVER_PREFIX
        })
        response = client.ping.get()
        print(response)
     
        df, reports = process.reports_to_pandas()
        df = df[df['list_name'].str.contains('OBPS News Letter')]
        df = df[df['subject_line'].str.contains('Newsletter')]
        df['date'] = pd.to_datetime(df['send_time'])
        df = df.sort_values(by=['date'])

        response_campaign = client.campaigns.list()
        df_grouth = process.growth_history_to_pandas('7c6ddfee90')
        df_grouth['date'] = pd.to_datetime(df_grouth['month'])
        df_grouth = df_grouth.replace({np.nan:None})
        df_location = process.locations_to_pandas('7c6ddfee90')
        
        # Manage content into database
        # Delete all dspace_records registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.newsletter;'''
        codes_list = db.delete_db(cursor, conn, query)       
        #  Insert into database if activity_code does not exist        
           
        for idx, row in df.iterrows():               
            arguments = {'str0':row['date'],
                         'str1':row['list_name'], 
                         'str2':row['subject_line'],
                         'str3':row['emails_sent'],
                         'str4':row['unsubscribed'],
                         'str5':row['hard_bounces'],    
                         'str6':row['soft_bounces'],
                         'str7':row['opens_total'],
                         'str8':row['unique_opens'],
                         'str9':row['clicks_total'],
                         'str10':row['unique_clicks'],
                         'str11':row['unique_subscriber_clicks'],
                         }  
            
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.newsletter
            (date, list_name, subject_line, emails_sent, unsubscribed, hard_bounces, soft_bounces, opens_total, unique_opens, clicks_total, unique_clicks, unique_subscriber_clicks ) 
            VALUES ( %(str0)s,%(str1)s, %(str2)s,%(str3)s,%(str4)s,%(str5)s,%(str6)s,%(str7)s,%(str8)s,%(str9)s,%(str10)s,%(str11)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(row['subject_line']) + '     written in database')     

        # Delete all dspace_records registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.newsletter_subscribers_grouth;'''
        codes_list = db.delete_db(cursor, conn, query)       
        #  Insert into database if activity_code does not exist        
           
        for idx, row in df_grouth.iterrows():               
            arguments = {'str0':row['date'],
                         'str1':row['subscribed'], 
                         'str2':row['list_change_mom'],
                         'str3':row['list_change_yoy'],

                         }  
            
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.newsletter_subscribers_grouth
            (date, subscribed, list_change_mom, list_change_yoy ) 
            VALUES ( %(str0)s,%(str1)s, %(str2)s,%(str3)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(row['date']) + '    subscribers grouth written in database')  
            
        
        # Delete all dspace_records registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.newsletter_locations;'''
        codes_list = db.delete_db(cursor, conn, query)       
        #  Insert into database if activity_code does not exist        
           
        for idx, row in df_location.iterrows():               
            arguments = {'str0':row['country'],
                         'str1':row['cc'], 
                         'str2':row['percent'],
                         'str3':row['total'],

                         }  
            
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.newsletter_locations
            (country, cc, percent, total ) 
            VALUES ( %(str0)s,%(str1)s, %(str2)s,%(str3)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(row['country']) + '    location info written in database')      
            
    except Exception:
        print(traceback.format_exc())    
