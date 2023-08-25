#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul  1 10:04:34 2023

@author: a33272
"""

from tika import parser # pip install tika
from urllib.request import urlopen
import pandas as pd
import re

docs_list = '/home/a33272/Documents/python/obps_metrics/data/doc_paths.csv'
df = pd.read_csv(docs_list)


for index, row in df.iterrows():
    try:
        doc = row.values[0]
        doc = re.sub("\s","%20", doc)
        doc_name = doc.rsplit('/', 1)[1]
        # Download from URL.
        with urlopen( 'https://repository.oceanbestpractices.org' + doc ) as webpage:
            content = webpage.read()
        
        # Save to file.
        with open( '/test_data/obps/' + doc_name, 'wb' ) as download:
            download.write( content )
    except Exception:
        pass
          
#dataset = {}    
#raw = parser.from_file('/home/a33272/Desktop/output.pdf')
#print(raw['content'])