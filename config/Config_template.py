#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar  8 17:56:37 2020

@author: Cristian Munoz Mas
"""

def get_settings():
    
    credentials = {}
    # metrics database settings
    credentials['db_metrics_dbname'] =      ''
    credentials['db_metrics_host'] =        ''
    credentials['db_metrics_port'] =        ''
    credentials['db_metrics_username'] =    ''
    credentials['db_metrics_password'] =    ''
    
    # google analytics API settings
    credentials['ga_scopes'] =              ''
    credentials['ga_key_file_location_searchengine'] =   ''
    credentials['ga_key_file_location_mainlandingpage'] =   ''        
    credentials['ga_view_id_searchengine'] =             ''
    credentials['ga_view_id_mainlandingpage'] =          ''       
    
    # Mailchimp API key
    credentials['mailchimp_api_key'] =  ''
    
    # dspace database settings
    credentials['db_dspace_dbname'] =      ''
    credentials['db_dspace_host'] =        ''
    credentials['db_dspace_port'] =        ''
    credentials['db_dspace_username'] =    ''
    credentials['db_dspace_password'] =    ''    

    # world cities file
    credentials['cities_world_path'] = ''

    return credentials
