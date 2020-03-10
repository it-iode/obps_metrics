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
    credentials['ga_key_file_location'] =   ''
    credentials['ga_view_id'] =             ''
    
    
    
    
    return credentials
