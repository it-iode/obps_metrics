#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 10:20:12 2023

@author: a33272
"""

from tika import parser # pip install tika
from urllib.request import urlopen
import pandas as pd
import re
import glob
import json

path_to_docs = '/test_data/obps/docs/*'
docs_list = glob.glob(path_to_docs)
dataset = {}  
for doc in docs_list:  
    raw = parser.from_file(doc)
    dataset[doc] = {'content': raw['content']}
    print(doc)
    

with open('/test_data/obps/obps_corpus.json', 'w') as fp:
    json.dump(dataset, fp)