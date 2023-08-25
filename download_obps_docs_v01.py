#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 12 17:07:42 2023

@author: a33272
"""

from tika import parser # pip install tika
from urllib.request import urlopen
import pandas as pd
import re
import glob
import json


docs_list = '/home/a33272/Documents/python/obps_metrics/data/doc_paths.csv'
df_docs = pd.read_csv(docs_list)
docs_list = '/home/a33272/Documents/python/obps_metrics/data/doc_maturity.csv'
df_trl = pd.read_csv(docs_list)


df_docs['handle'] = df_docs['doc_path'].str.extract(r'\/bitstream\/handle\/([0-9]+\/[0-9]+)\/.*')
df = pd.merge(df_docs, df_trl, on='handle', how='inner')

dataset = {}
dataset_trl = {}

# for index, row in df_docs.iterrows():
#     try:
#         doc = row.values[0]
#         doc = re.sub("\s","%20", doc)
#         doc_name = doc.rsplit('/', 1)[1]
#         # Download from URL.
#         url = 'https://repository.oceanbestpractices.org' + doc 
#         raw = parser.from_file(url)
#         dataset[doc] = {'content': raw['content'],
#                         'handle': row['handle']}
        
#     except Exception:
#         pass

# with open('/test_data/obps/obps_corpus.json', 'w') as fp:
#     json.dump(dataset, fp)

for index, row in df.iterrows():
    try:
        doc = row.values[0]
        doc = re.sub("\s","%20", doc)
        doc_name = doc.rsplit('/', 1)[1]
        # Download from URL.
        url = 'https://repository.oceanbestpractices.org' + doc 
        raw = parser.from_file(url)
        dataset_trl[doc] = {
                        'dspace_object_id': row['dspace_object_id'],
                        'content': raw['content'],
                        'handle': row['handle'],
                        'maturity_level_rank':row['maturity_level_rank'],
                        'abstract':row['abstract'],
                        'title':row['title']
                        }
        
    except Exception:
        pass

with open('/test_data/obps/obps_corpus_trl.json', 'w') as fp:
    json.dump(dataset_trl, fp)
