import re
import sys
import time
import pandas

import app.query
import app.entity
import app.relation
import app.topic

from pymongo import MongoClient
from tqdm import tqdm
from datetime import datetime

# This project requires lucene apache, for installation please follow https://lucene.apache.org/pylucene/jcc/install.html
# The version that we are currently using is 8.11.0 on Java 1.8
# We should note that this project was tested on Ubuntu 18.04

# Please change the ip address
CONNECTION_STRING = ""
client = MongoClient(CONNECTION_STRING)

dbname = None
collectionname = None
allpmcids = []
try:
    _ = client.server_info()
    dbname = client['cellline']
    collectionname = dbname['PMCCollection']
    for obj in collectionname.find({}, {'PMCID':1}):
        allpmcids.append(obj['PMCID'])
except:
    print("Connection Failed")


if (not dbname == None) & (not collectionname == None):
    print("1. Database Initialization... Success!")
    print("2. Updating PMCID...")
    check = False
    pmcids = []
    
    # Set the date (publication) and file of queries (per line describes one cell line) for retrieval
    result = app.query.get_based_on_query('2023/01', 'resource/samplequeries.txt')
    if (result[0] == 'Success'):
        pmcids = result[1]
        pmcids = list(set(pmcids)-set(allpmcids))
        if (len(pmcids) > 0):
            rows = []
            print("3. Downloading PMC XML...")
            try:
                succeed = 0
                failed = 0
                files = []
                for pmcid in tqdm(pmcids):
                    result = app.query.get_based_on_identifier(pmcid)
                    if (result[0] == 'Success'):
                        succeed += 1
                        files.append(pmcid)
                        time.sleep(2)
                    else:
                        failed += 1
                
                print("{0} documents downloaded!".format(succeed))
                
                succeed = 0
                failed = 0
                labelsorted = app.query.get_label_section_pubmed()
                for pmcid in tqdm(files):
                    result = app.query.post_pmc_document_mongo(pmcid, collectionname, labelsorted)
                    if (result[0] == 'Success'):
                        for t in result[1]:
                            rows.append({'fileid': pmcid, 'id': 'TITLE', 'label': "['TITLE']", 'sectype': 'TITLE', 'text': t})
                        for a in result[2]:
                            rows.append({'fileid': pmcid, 'id': a, 'sectype': result[2][a][1], 'label': result[2][a][2], 'text': result[2][a][0]})
                        for b in result[3]:
                            rows.append({'fileid': pmcid,  'id': b, 'sectype': result[3][b][1], 'label': result[3][b][2], 'text': result[3][b][0]})
                        succeed += 1
                    else:
                        failed += 1
                
                if (len(rows) > 0):
                    dataframe = pandas.DataFrame(rows)
                    dataframe.to_csv('tsv/downloaded.tsv', sep='\t')
                    print("Added new {0} documents to database...".format(succeed))
                    
                    rows = []
                    sentenceid = 0
                    for idx, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0]):
                        if (len(rows) > 0):
                            if (rows[-1]['PMCID'] != row['fileid']):
                                sentenceid = 0
                        if (not row['sectype'] == 'TITLE') & (not row['sectype'] == 'UNUSED') & (not pandas.isnull(row['text'])):
                            text = row['text']
                            candidates = []
                            brackets = re.findall(r'\([^\(\)]+[\(\)0-9A-Za-z\.]*[^\(\)]+\)', text)
                            brackets.extend(re.findall(r'\[[^\[\]]+[\(\)0-9A-Za-z\.]*[^\[\]]+\]', text))
                            brackets.extend(re.findall(r'([A-Z][a-z]+\.){2,}', text))
                            candidates.extend([c for bracket in brackets for c in re.findall(r'[^\s\.]+\.', bracket)])
                            candidates.extend(['e.g.', 'i.e.', 'et al.', 'approx.'])
                            candidates = list(set(candidates))
                            sentences = app.query.get_sentences(text, candidates)
                            for sent in sentences:
                                rows.append({'PMCID': row['fileid'], 'id': row['id'], 'sectype': row['sectype'], 'sentenceid': sentenceid, 'text': sent})
                                sentenceid += 1
                        elif (row['sectype'] == 'TITLE') & (not row['sectype'] == 'UNUSED') & (not pandas.isnull(row['text'])):
                            rows.append({'PMCID': row['fileid'], 'id': row['id'], 'sectype': row['sectype'], 'sentenceid': sentenceid, 'text': row['text']})
                            sentenceid += 1
                    dataframe = pandas.DataFrame(rows)
                    dataframe.to_csv('tsv/sentences.tsv', sep='\t')
                check = True
            except:
                check = False
                print("Failure in processing PMC files...\nExiting...")
            
            if (check):
                print("4. Updating entity (NER) information...")
                [nlp, indexer] = app.entity.initializing_model()
                [entities, dataframe] = app.entity.get_ner_information(nlp, dataframe, indexer)
                for pmcid in tqdm(entities):
                    for obj in collectionname.find({'PMCID': pmcid}):
                        try:
                            _ = collectionname.update_one({'_id': obj['_id']}, {'$set': {'SENTENCES': entities[pmcid], 'NER_updatetime': datetime.now().strftime('%Y%m%d %H:%M:%s')}}, upsert=False)
                        except:
                            pass
                
                print("5. Updating relation (knowledge graph) information...")
                nlp = app.relation.initializing_model()
                graphs = app.relation.get_relation_information(nlp, dataframe)
                for pmcid in tqdm(graphs):
                    for obj in collectionname.find({'PMCID': pmcid}):
                        try:
                            _ = collectionname.update_one({'_id': obj['_id']}, {'$set': {'RELATIONS': graphs[pmcid], 'RELATION_updatetime': datetime.now().strftime('%Y%m%d %H:%M:%s')}}, upsert=False)
                        except:
                            pass
                
                print("6. Updating topic and keyphrase information...")
                app.topic.run_topic_and_keyphrase_extraction(collectionname)
        else:
            print("No New Record Found")
    else:
        print("Failure in updating PMC files...\nExiting...")
    
    client.close()

