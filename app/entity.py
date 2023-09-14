import torch
import lucene
import pandas
import itertools
import re

from org.apache.lucene import analysis, document, index, queryparser, search, store
from lupyne import engine
from transformers import AutoTokenizer, AutoModelForTokenClassification, AutoConfig, pipeline
from tqdm import tqdm

assert lucene.getVMEnv() or lucene.initVM()

ids_to_labels = {'LABEL_0': 'B-DNA', 'LABEL_1': 'B-RNA', 'LABEL_2': 'B-cell_line', 'LABEL_3': 'B-cell_type', 'LABEL_4': 'B-disease', 'LABEL_5': 'B-drug', 'LABEL_6': 'B-gene', 'LABEL_7': 'B-tissue', 'LABEL_8': 'I-DNA', 'LABEL_9': 'I-RNA', 'LABEL_10': 'I-cell_line', 'LABEL_11': 'I-cell_type', 'LABEL_12': 'I-disease', 'LABEL_13': 'I-drug', 'LABEL_14': 'I-gene','LABEL_15': 'I-tissue', 'LABEL_16': 'O'}

pairs = [('gene', 'gene'), ('drug', 'gene'), ('gene', 'drug'), ('drug', 'disease'), ('disease', 'drug'), ('gene', 'disease'), ('disease', 'gene'), ('tissue', 'gene'), ('gene', 'tissue'), ('cell_line', 'gene'), ('cell_line', 'cell_type'), ('cell_line', 'disease'), ('disease', 'cell_line'), ('disease', 'tissue'), ('tissue', 'cell_line')]
masknames = {'gene': 'PROTEIN', 'drug': 'DRUG', 'disease': 'DISORDER', 'tissue': 'TISSUE', 'cell_line': 'CELL', 'cell_type': 'CELL'}

def initializing_model():
    indexer = engine.Indexer('resource/index')
    folder = 'model/ner'
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    tokenizer = AutoTokenizer.from_pretrained(folder, do_lower_case=True, local_files_only=True, padding='max_length', truncation=True, model_max_length=256)
    config = AutoConfig.from_pretrained(folder)
    model = AutoModelForTokenClassification.from_config(config)
    state_dict = torch.load('{0}/pytorch_model.bin'.format(folder), map_location=torch.device('cpu'))
    model.load_state_dict(state_dict)
    _ = model.to(device)
    nlp = pipeline('ner', model=model, tokenizer=tokenizer)
    return [nlp, indexer]

def get_ner_information(nlp, dataframe, indexer):
    allentities = {}
    rows = []
    for idx, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0]):
        result = nlp(row['text'])
        entities = []
        check = False
        for i in range(len(result)):
            if (result[i]['word'].startswith('##')) & (len(entities) > 0) & (check == True):
                entities[-1]['end'] = result[i]['end']
                entities[-1]['word'] += re.sub(r'^##', '', result[i]['word'])
            elif (ids_to_labels[result[i]['entity']].startswith('B-')):
                entities.append(result[i].copy())
                entities[-1]['entity'] = ids_to_labels[result[i]['entity']]
                check = True
            elif (ids_to_labels[result[i]['entity']].startswith('I-')):
                if (i-1 > -1) & (len(entities) > 0) & (check == True):
                    entities[-1]['end'] = result[i]['end']
                    entities[-1]['word'] += re.sub(r'^##', '', result[i]['word'])
            elif (ids_to_labels[result[i]['entity']] == 'O'):
                check = False
        tmp = {}
        nentities = []
        if (len(entities) > 0):
            for e in entities:
                entity = re.sub(r'[^\w]+', '', row['text'][e['start']:e['end']].lower())
                hits = indexer.search('text:"{0}"'.format(entity), 5)
                label = []
                ids = []
                for h in hits:
                    ids.append(h['ids'])
                    label.append(h['label'])
                
                if (ids == []):
                    ids = ['CUI-less']
                    label = row['text'][e['start']:e['end']].lower()
                tmp[tuple([e['start'], e['end']])] = {'entity': e['entity'].split('-')[1], 'normalized-ids': ids, 'label': label}
                nentities.append({'start': e['start'], 'end': e['end'], 'label': e['entity'], 'text': row['text'][e['start']:e['end']], 'normalized-id':ids})
            
            for pair in itertools.combinations(list(tmp.keys()), 2):
                check = False
                if (tuple([tmp[pair[0]]['entity'], tmp[pair[1]]['entity']]) in pairs):
                    if ('CUI-less' in tmp[pair[0]]['normalized-ids']) | ('CUI-less' in tmp[pair[1]]['normalized-ids']):
                        if (not tmp[pair[0]]['label'] == tmp[pair[1]]['label']):
                            check = True
                    elif (not tmp[pair[0]]['normalized-ids'] == tmp[pair[1]]['normalized-ids']):
                        check = True
                if (check):
                    newsentence = row['text'][0:pair[0][0]]
                    newsentence += '[{0}]'.format(masknames[tmp[pair[0]]['entity']])
                    newsentence += row['text'][pair[0][1]:pair[1][0]]
                    newsentence += '[{0}]'.format(masknames[tmp[pair[1]]['entity']])
                    newsentence += row['text'][pair[1][1]:]
                    rows.append({'PMCID': row['PMCID'], 'id': row['id'], 'sectype': row['sectype'], 'text': newsentence, 'right-type': tmp[pair[0]]['entity'], 'right-ids': tmp[pair[0]]['normalized-ids'], 'right-label': tmp[pair[0]]['label'], 'left-type': tmp[pair[1]]['entity'], 'left-ids': tmp[pair[1]]['normalized-ids'], 'left-label': tmp[pair[1]]['label']})
        try:
            allentities[row['PMCID']][row['sectype']] = []
        except:
            allentities[row['PMCID']] = {}
            allentities[row['PMCID']][row['sectype']] = []
        
        allentities[row['PMCID']][row['sectype']].append({'sentence-id': row['sentenceid'], 'id': row['id'], 'sentence': row['text'], 'entities': nentities})
        
    if (len(rows) > 0):
        dataframe = pandas.DataFrame(rows)
        dataframe.to_csv('tsv/sentences-for-relation.tsv', sep='\t')
    
    indexer.close()
    return [allentities, dataframe]
