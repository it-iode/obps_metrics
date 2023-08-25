#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 16:47:09 2022

@author: a33272
"""
import pandas as pd
import psycopg2
import logging
import numpy as np
import matplotlib.pyplot as plt
import utils.db_connect as db
import country_list

class DspaceAudit():
    def __init__(self):
        
        self.db_config = {
        'server': 'localhost',
        'name': 'obps_dspace_20221123',
        'user': 'postgres',
        'password': 'postgres',
        'port': '5432'
        }
        
        self.cities_world_path = '/home/a33272/Documents/python/obps_metrics/config/worldcities.csv'
        
        self.mappingnavn_map = {'LOG': 'log',
                                
                                
                                }
            

    def connect_db(self, settings):
        conn = psycopg2.connect(dbname=settings['name'],host=settings['server'],
                                port=settings['port'], user=settings['user'], 
                                password=settings['password']) #define connection
        logging.info('Connecting to database\n	->%s' % (conn))
        cursor = conn.cursor()  # conn.cursor will return a cursor object, you can use this cursor to perform queries
        logging.info('Connected!\n')
        return cursor, conn;    
    
    def query_db(self, cursor, conn, query):    
        cursor.execute(query)
        query_output = cursor.fetchall()
        cursor.close()
        conn.close()
        return query_output;

    def plot_pie_chart(self, data, title):
        df = pd.DataFrame (data, columns = ['category'])
        df_count = df.groupby(['category']).size().to_frame('size')
        df_count = df_count.reset_index(drop=False)
        #explode = (0,0,0, 0, 0, 0.1, 0.1, 0.2, 0.3, 0.4, 0.6)
        pie = df_count.plot.pie(y='size', autopct='%1.0f%%', figsize=(15,10), fontsize=17, labels=df_count['category'], legend=False)
        #pie.legend(loc="best")
        #pie = df_count.plot.pie(y='size')
        fig = pie.get_figure()
        fig.savefig('/home/a33272/Documents/obps/dspace_audit_test/' + title + '.png')
        return pie

    def import_cities_world(self, cities_world_path):
        df = pd.read_csv(self.cities_world_path)
        return df
    
if __name__ == '__main__':
    try:        
        # Initialize related processing classes
        processing = DspaceAudit()   


        # Dspace records
        query = '''
        SELECT handle,
        metadatavalue1.dspace_object_id as dspace_object_id,
        metadatavalue1.text_value as title,
        metadatavalue2.text_value as abstract, 
        metadatavalue3.text_value as date_submitted,
        metadatavalue4.text_value as maturity_level,
        metadatavalue7.text_value as doi,
        doi.doi as doi_obp,
        item1.submitter_id,
        metadatavalue8.text_value as publisher_place,
		item3.last_modified as last_modified,       
        metadatavalue9.text_value as year_created		
        FROM handle 
        LEFT JOIN metadatavalue AS metadatavalue1
        ON handle.resource_id = metadatavalue1.dspace_object_id AND metadatavalue1.metadata_field_id=64
        LEFT JOIN metadatavalue AS metadatavalue2
        ON handle.resource_id = metadatavalue2.dspace_object_id AND metadatavalue2.metadata_field_id=27
        LEFT JOIN metadatavalue AS metadatavalue3
        ON handle.resource_id = metadatavalue3.dspace_object_id AND metadatavalue3.metadata_field_id=11
        LEFT JOIN metadatavalue AS metadatavalue4
        ON handle.resource_id = metadatavalue4.dspace_object_id AND metadatavalue4.metadata_field_id=164
        LEFT JOIN metadatavalue AS metadatavalue7
        ON handle.resource_id = metadatavalue7.dspace_object_id AND metadatavalue7.metadata_field_id=140
        LEFT JOIN doi
        ON doi.dspace_object = metadatavalue1.dspace_object_id
        LEFT JOIN item AS item1
        ON item1.uuid = metadatavalue1.dspace_object_id
        LEFT JOIN item AS item2
        ON item2.uuid = metadatavalue1.dspace_object_id
        LEFT JOIN metadatavalue AS metadatavalue8
        ON handle.resource_id = metadatavalue8.dspace_object_id AND metadatavalue8.metadata_field_id=137
        LEFT JOIN item AS item3
        ON item3.uuid = metadatavalue1.dspace_object_id   
        LEFT JOIN metadatavalue AS metadatavalue9
        ON handle.resource_id = metadatavalue9.dspace_object_id AND metadatavalue9.metadata_field_id=15
        WHERE NOT
        (handle.resource_id IS NULL 
          OR metadatavalue1.text_value IS NULL 
          OR metadatavalue3.text_value IS NULL
          OR item2.discoverable = false
        );
        '''
        cursor, conn = processing.connect_db(processing.db_config)
        query_dspace = processing.query_db(cursor, conn, query)
        df = pd.DataFrame (query_dspace, columns = ['handle', 'dspace_object_id', 'title', 'abstract', 'date_submitted', 'maturity_level', 'doi', 'doi_obp', 'submitter_id', 'publisher_place', 'last_modified', 'year_created'])
        
        df['country'] = ''
        df_cities = processing.import_cities_world(processing.cities_world_path)
        countries_dict = dict(country_list.countries_for_language('en'))
        us_states_dict = {
            'AK': 'Alaska',
            'AL': 'Alabama',
            'AR': 'Arkansas',
            'AZ': 'Arizona',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DC': 'District of Columbia',
            'DE': 'Delaware',
            'FL': 'Florida',
            'GA': 'Georgia',
            'HI': 'Hawaii',
            'IA': 'Iowa',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'MA': 'Massachusetts',
            'MD': 'Maryland',
            'ME': 'Maine',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MO': 'Missouri',
            'MS': 'Mississippi',
            'MT': 'Montana',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'NE': 'Nebraska',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NV': 'Nevada',
            'NY': 'New York',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VA': 'Virginia',
            'VT': 'Vermont',
            'WA': 'Washington',
            'WI': 'Wisconsin',
            'WV': 'West Virginia',
            'WY': 'Wyoming'
        }
        # Global countries
        for i in range(len(df)) :
            if df.loc[i, 'publisher_place']:
                res = [val for key, val in countries_dict.items() if val.lower() in df.loc[i, 'publisher_place'].lower()]
                if res:
                    df.loc[i, 'country'] = res[0]
        # United Kingdom            
        for i in range(len(df)) :
            if df.loc[i, 'publisher_place']:
                names_list = ['England', 'Scotland', 'UK']
                if any(n in df.loc[i, 'publisher_place'] for n in names_list):
                    df.loc[i, 'country'] = 'United Kingdom'
        # US states            
        for i in range(len(df)) :
            if df.loc[i, 'publisher_place']:
                res = [key for key, val in us_states_dict.items() if key in df.loc[i, 'publisher_place']]
                if res:
                    df.loc[i, 'country'] = 'United States'
        for i in range(len(df)) :
            if df.loc[i, 'publisher_place']:
                res = [val for key, val in us_states_dict.items() if val in df.loc[i, 'publisher_place']]
                if res:
                    df.loc[i, 'country'] = 'United States'
        # Other cities            
        for i in range(len(df)) :
            if df.loc[i, 'publisher_place']:
                if not df.loc[i, 'country']:
                    if 'Paris' in  df.loc[i, 'publisher_place']:
                        df.loc[i, 'country'] = 'France'
                    elif 'Geneva' in  df.loc[i, 'publisher_place']:
                        df.loc[i, 'country'] = 'Switzerland'
                    elif 'Hobart' in  df.loc[i, 'publisher_place']:
                        df.loc[i, 'country'] = 'Australia'
                    elif 'Perth' in  df.loc[i, 'publisher_place']:
                        df.loc[i, 'country'] = 'Australia'             
                    elif 'Zanzibar' in  df.loc[i, 'publisher_place']:
                        df.loc[i, 'country'] = 'Tanzania'   
                    elif 'México' in  df.loc[i, 'publisher_place']:
                        df.loc[i, 'country'] = 'Mexico'                   
                    elif 'Ottawa' in  df.loc[i, 'publisher_place']:
                        df.loc[i, 'country'] = 'Canada'  
                    elif 'Nunavut' in  df.loc[i, 'publisher_place']:
                        df.loc[i, 'country'] = 'Canada'   
            else:   
                df.loc[i, 'country'] = 'Unknown or unspecified country'                     
                            
               
        # Delete all dspace_records registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.dspace_records;'''
        codes_list = db.delete_db(cursor, conn, query)
        
        #  Insert into database if activity_code does not exist        
        for i in range(len(df)) :
                    
            arguments = {'str0':df.loc[i, 'handle'],
                          'str1':df.loc[i, 'title'], 
                          'str2':df.loc[i, 'abstract'],
                          'str3':df.loc[i, 'maturity_level'],
                          'str4':df.loc[i, 'date_submitted'],
                          'str7':df.loc[i, 'doi'],
                          'str8':df.loc[i, 'doi_obp'],
                          'str9':df.loc[i, 'submitter_id'],
                          'str10':df.loc[i, 'publisher_place'],
                          'str11':df.loc[i, 'country'],
                          'str12':df.loc[i, 'last_modified'],
                          'str13':df.loc[i, 'year_created'],
                          'str14':df.loc[i, 'dspace_object_id'],
                          }

            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.dspace_records 
            (handle, title, abstract, maturity_level, date_submitted, doi, doi_obp, submitter_id, publisher_place, country, last_modified, year_created, dspace_object_id) 
            VALUES ( %(str0)s,%(str1)s, %(str2)s,%(str3)s,%(str4)s,%(str7)s,%(str8)s,%(str9)s,%(str10)s,%(str11)s,%(str12)s,%(str13)s,%(str14)s);
            '''         
            
            db.write_db(cursor, conn, query, arguments)  
            print(str(df.loc[i, 'handle']) + '     record written in database')       


        # Dspace collections
        query = '''
        SELECT text_value
        FROM metadatavalue, collection
        WHERE collection.uuid = metadatavalue.dspace_object_id and metadatavalue.metadata_field_id=64
        '''
        cursor, conn = processing.connect_db(processing.db_config)
        query_trl = processing.query_db(cursor, conn, query)        
        cursor, conn = processing.connect_db(processing.db_config)
        query_dspace = processing.query_db(cursor, conn, query)
        df = pd.DataFrame (query_dspace, columns = ['title'])
        # Delete all dspace_records registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.dspace_collections;'''
        codes_list = db.delete_db(cursor, conn, query)       
        #  Insert into database if activity_code does not exist        
        for i in range(len(df)) :                   
            arguments = {'str0':df.loc[i, 'title'], }       
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.dspace_collections 
            (title) 
            VALUES ( %(str0)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(df.loc[i, 'title']) + '     collection written in database')        
  
      
        # Dspace communities
        query = '''
        SELECT text_value
        FROM metadatavalue, community
        WHERE community.uuid = metadatavalue.dspace_object_id and metadatavalue.metadata_field_id=64
        '''
        cursor, conn = processing.connect_db(processing.db_config)
        query_trl = processing.query_db(cursor, conn, query)        
        cursor, conn = processing.connect_db(processing.db_config)
        query_dspace = processing.query_db(cursor, conn, query)
        df = pd.DataFrame (query_dspace, columns = ['title'])
        # Delete all dspace_records registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.dspace_communities;'''
        codes_list = db.delete_db(cursor, conn, query)       
        #  Insert into database if activity_code does not exist        
        for i in range(len(df)) :                  
            arguments = {'str0':df.loc[i, 'title'], }       
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.dspace_communities
            (title) 
            VALUES ( %(str0)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(df.loc[i, 'title']) + '     community written in database')          


        # Discipline
        query = 'select dspace_object_id, text_value from metadatavalue where metadata_field_id = 141 AND text_value LIKE \'Parameter Discipline::%\''
        cursor, conn = processing.connect_db(processing.db_config)
        query_discipline = processing.query_db(cursor, conn, query) 
        df = pd.DataFrame (query_discipline, columns = ['dspace_object_id', 'discipline_name']) 
        df['discipline_name'] = df['discipline_name'].str.replace('Parameter Discipline::', '') # Remove Parameter Discipline
        df['discipline_name'] = df['discipline_name'].str.split('::').str[0] # Remove Parameter discipline after 
        df = df.drop_duplicates(subset = ['dspace_object_id', 'discipline_name'],keep = 'first').reset_index(drop = True)        
        # Delete all dspace_record_disciplines registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.dspace_record_disciplines;'''
        codes_list = db.delete_db(cursor, conn, query)  
        #  Insert into database        
        for i in range(len(df)) :                  
            arguments = {'str0':df.loc[i, 'dspace_object_id'], 
                         'str1':df.loc[i, 'discipline_name'],
                         }       
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.dspace_record_disciplines
            (dspace_object_id, discipline_name) 
            VALUES ( %(str0)s, %(str1)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(df.loc[i, 'discipline_name']) + '     written in database')    


        # Best Practice type
        query = 'select dspace_object_id, text_value from metadatavalue where metadata_field_id = 165'
        cursor, conn = processing.connect_db(processing.db_config)
        query_bptype = processing.query_db(cursor, conn, query) 
        df = pd.DataFrame (query_bptype, columns = ['dspace_object_id', 'bp_type']) 
      
        # Delete all dspace_record_disciplines registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.dspace_record_bptypes;'''
        codes_list = db.delete_db(cursor, conn, query)  
        #  Insert into database        
        for i in range(len(df)) :                  
            arguments = {'str0':df.loc[i, 'dspace_object_id'], 
                         'str1':df.loc[i, 'bp_type'],
                         }       
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.dspace_record_bptypes
            (dspace_object_id, bp_type) 
            VALUES ( %(str0)s, %(str1)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(df.loc[i, 'bp_type']) + '     written in database')  


        # Adoption
        query = 'select dspace_object_id, text_value from metadatavalue where metadata_field_id = 168'
        cursor, conn = processing.connect_db(processing.db_config)
        query_adoptiontype = processing.query_db(cursor, conn, query) 
        df = pd.DataFrame (query_adoptiontype, columns = ['dspace_object_id', 'adoption_type']) 
      
        # Delete all dspace_record_disciplines registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.dspace_record_adoptiontypes;'''
        codes_list = db.delete_db(cursor, conn, query)  
        #  Insert into database        
        for i in range(len(df)) :                  
            arguments = {'str0':df.loc[i, 'dspace_object_id'], 
                         'str1':df.loc[i, 'adoption_type'],
                         }       
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.dspace_record_adoptiontypes
            (dspace_object_id, adoption_type) 
            VALUES ( %(str0)s, %(str1)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(df.loc[i, 'adoption_type']) + '     written in database')  

        # EOVs
        query = 'select dspace_object_id, text_value from metadatavalue where metadata_field_id = 163'
        cursor, conn = processing.connect_db(processing.db_config)
        query_adoptiontype = processing.query_db(cursor, conn, query) 
        df = pd.DataFrame (query_adoptiontype, columns = ['dspace_object_id', 'eov']) 
      
        # Delete all dspace_record_disciplines registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.dspace_record_eovs;'''
        codes_list = db.delete_db(cursor, conn, query)  
        #  Insert into database        
        for i in range(len(df)) :                  
            arguments = {'str0':df.loc[i, 'dspace_object_id'], 
                         'str1':df.loc[i, 'eov'],
                         }       
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.dspace_record_eovs
            (dspace_object_id, eov) 
            VALUES ( %(str0)s, %(str1)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(df.loc[i, 'eov']) + '    EOV written in database')  

        # SDGs
        query = 'select dspace_object_id, text_value from metadatavalue where metadata_field_id = 162'
        cursor, conn = processing.connect_db(processing.db_config)
        query_adoptiontype = processing.query_db(cursor, conn, query) 
        df = pd.DataFrame (query_adoptiontype, columns = ['dspace_object_id', 'sdg']) 
      
        # Delete all dspace_record_disciplines registers from database
        cursor, conn = db.connect_db()
        query = '''DELETE FROM metrics.dspace_record_sdgs;'''
        codes_list = db.delete_db(cursor, conn, query)  
        #  Insert into database        
        for i in range(len(df)) :                  
            arguments = {'str0':df.loc[i, 'dspace_object_id'], 
                         'str1':df.loc[i, 'sdg'],
                         }       
            cursor, conn = db.connect_db()
            query = '''INSERT INTO metrics.dspace_record_sdgs
            (dspace_object_id, sdg) 
            VALUES ( %(str0)s, %(str1)s);
            '''                    
            db.write_db(cursor, conn, query, arguments)  
            print(str(df.loc[i, 'sdg']) + '    SDG written in database')  
            
## --------------------------------------------------------------------------------------------------------
            
        # Adoption
        adoption_cat_list = ['Multi-organisational', 'Organisational', 'National', 'Novel (no adoption outside originators)', 'International', 'Validated (tested by third parties)']
        adoption_list = []
        query = 'select text_value from metadatavalue where metadata_field_id = 168'
        cursor, conn = processing.connect_db(processing.db_config)
        query_adoption = processing.query_db(cursor, conn, query) 
        for record in query_adoption:
            for add in adoption_cat_list:
                if add == record[0]:
                    adoption_list.append(add)   






    
        # TRLs
        trl_cat_list = ['TRL 1', 'TRL 2', 'TRL 3', 'TRL 4', 'TRL 5', 'TRL 6', 'TRL 7', 'TRL 8', 'TRL 9']
        trl_list = []
        count_old_trl_records=0
        query = 'select text_value from metadatavalue where metadata_field_id = 164'
        cursor, conn = processing.connect_db(processing.db_config)
        query_trl = processing.query_db(cursor, conn, query)
        for record in query_trl:
            for trl in trl_cat_list:
                if trl in record[0][0:5]:
                    trl_list.append(trl)
                    count_old_trl_records = count_old_trl_records + 1
        
        plot = processing.plot_pie_chart(trl_list, 'obps_trl')      

        # TRLs New
        trl_cat_list = ['N/A', 
                        'Mature', 
                        'Pilot', 
                        'Concept']
        trl_list = []
        count_new_trl_records=0
        query = 'select text_value from metadatavalue where metadata_field_id = 164'
        cursor, conn = processing.connect_db(processing.db_config)
        query_trl = processing.query_db(cursor, conn, query)
        for record in query_trl:
            for trl in trl_cat_list:
                if record[0].startswith(trl):
                    trl_list.append(trl)
                    count_new_trl_records = count_new_trl_records + 1
        
        plot = processing.plot_pie_chart(trl_list, 'obps_trl_new')   
        
        # Best Practice type
        bptype_cat_list = ['Guide', 'Training and Educational Material', 'Standard', 'Manual', 'Best Practice', 'Standard Operating Procedure']
        bptype_list = []
        query = 'select text_value from metadatavalue where metadata_field_id = 165'
        cursor, conn = processing.connect_db(processing.db_config)
        query_bptype = processing.query_db(cursor, conn, query)        
        for record in query_bptype: 
            for bptype in bptype_cat_list:
                if (bptype == 'Standard') and (len(record[0]) == 8) and (bptype in record[0]):
                    bptype_list.append(bptype)
                elif (bptype == 'Standard Operating Procedure') and (bptype in record[0]):
                    bptype_list.append(bptype)
                elif (bptype != 'Standard') and (bptype != 'Standard Operating Procedure') and (bptype in record[0]):
                    bptype_list.append(bptype)
        plot = processing.plot_pie_chart(bptype_list, 'obps_bptype')         

        # SDGs
        sdg_cat_list = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18']
        sdg_list = []
        query = 'select text_value from metadatavalue where metadata_field_id = 162'
        cursor, conn = processing.connect_db(processing.db_config)
        query_sdg = processing.query_db(cursor, conn, query)
        for record in query_sdg:
            for sdg in sdg_cat_list:
                if sdg == record[0].split('.')[0]:
                    sdg_list.append(sdg)        
        plot = processing.plot_pie_chart(sdg_list, 'obps_sdg') 
        
        # EOVs
        eov_cat_list = ['N/A', 'Ocean colour', 'Sea state', 'Zooplankton biomass and diversity', 'Subsurface currents',
                        'Phytoplankton biomass and diversity','Sea surface height', 'Sea surface temperature',
                        'Subsurface temperature', 'Surface currents', 'Sea surface salinity', 'Subsurface salinity',
                        'Ocean surface stress', 'Ocean surface heat flux', 'Biotoxins / Phycotoxins', 'Oxygen',
                        'Seagrass Cover and composition', 'Ocean sound', 'Fish abundance and distribution',
                        'Benthic invertebrate abundance and distribution', 'Macroalgal canopy cover and composition',
                        'Mangroove cover and composition', 'Inorganic carbon', 'Hard coral cover and composition', 'stable carbon isotopes',
                        'Dissolved organic carbon', 'Particulate matter', 'Nutrients', 'Marine turtles, birds, mammals abundance and distribution',
                        'transient tracers', 'Marine debris', 'Nitrous oxide', 'Microbe biomass and diversity', 'Sea ice', 'pH', ]
        eov_list = []
        query = 'select text_value from metadatavalue where metadata_field_id = 163'
        cursor, conn = processing.connect_db(processing.db_config)
        query_eov = processing.query_db(cursor, conn, query)        
        for record in query_eov:
            for eov in eov_cat_list:
                if eov.lower() == record[0].lower():
                    eov_list.append(eov)        
        plot = processing.plot_pie_chart(eov_list, 'obps_eov')  
        
        # Adoption
        adoption_cat_list = ['Multi-organisational', 'Organisational', 'National', 'Novel (no adoption outside originators)', 'International', 'Validated (tested by third parties)']
        adoption_list = []
        query = 'select text_value from metadatavalue where metadata_field_id = 168'
        cursor, conn = processing.connect_db(processing.db_config)
        query_adoption = processing.query_db(cursor, conn, query) 
        for record in query_adoption:
            for add in adoption_cat_list:
                if add == record[0]:
                    adoption_list.append(add)        
        plot = processing.plot_pie_chart(adoption_list, 'obps_adoption')         
        
        # Discipline
        discipline_cat_list = ['Cross-discipline', 'Atmosphere', 'Biological oceanography', 
                                'Chemical oceanography','Environment', 'Fisheries and aquaculture',
                                'Marine geology', 'Physical oceanography', 'Terrestrial', 'Administration and dimensions',
                                'Human activity']
        discipline_list = []
        query = 'select text_value from metadatavalue where metadata_field_id = 141'
        cursor, conn = processing.connect_db(processing.db_config)
        query_discipline = processing.query_db(cursor, conn, query) 
        for record in query_discipline:
            for add in discipline_cat_list:
                if add in record[0]:
                    discipline_list.append(add)        
        plot = processing.plot_pie_chart(discipline_list, 'obps_discipline')  

        # Methodology Type
        methtype_cat_list = ['Method', 'Specification of criteria', 'Guidelines & Policies',
                              'Reports with methodological relevance', 'Best Practice',
                              'Training/Educational material', 'Description of a metrology standard']
        methtype_list = []
        query = 'select text_value from metadatavalue where metadata_field_id = 173'
        cursor, conn = processing.connect_db(processing.db_config)
        query_methtype = processing.query_db(cursor, conn, query) 
        for record in query_methtype:
            for add in methtype_cat_list:
                if add == record[0]:
                    methtype_list.append(add)        
        plot = processing.plot_pie_chart(methtype_list, 'obps_methodology_type')   


        # Endorsement Type
        
        
        methtype_cat_list = ["deJureStandard","deFactoStandard","goodPractice","recommendedPractice","bestPractice"]
        methtype_list = []
        query = 'select text_value from metadatavalue where metadata_field_id IN (174,175,176,177,178)'
        cursor, conn = processing.connect_db(processing.db_config)
        query_methtype = processing.query_db(cursor, conn, query) 
        for record in query_methtype:
            for add in methtype_cat_list:
                if add == record[0]:
                    methtype_list.append(add)        
        plot = processing.plot_pie_chart(methtype_list, 'obps_authEndorsement_type')          
        
    except Exception:
        logging.getLogger().error("Fatal error in processing_track", exc_info=True) 
        
        
        
        
        
        
