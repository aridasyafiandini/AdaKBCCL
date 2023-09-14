import torch
import pandas
import re

from transformers import AutoTokenizer, AutoModelForSequenceClassification, AutoConfig, pipeline
from tqdm import tqdm

def initializing_model():
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    tokenizer = AutoTokenizer.from_pretrained('model/re', local_files_only=True, padding='max_length', truncation=True, model_max_length=256)
    special_tokens_dict = {'additional_special_tokens': ['[DISORDER]', '[DRUG]', '[PROTEIN]', '[CELL]', '[TISSUE]']}
    num_added_toks = tokenizer.add_special_tokens(special_tokens_dict)
    checkpoint = torch.load('model/re/re.pth', map_location=torch.device('cpu'))
    config = AutoConfig.from_pretrained('model/re')
    model = AutoModelForSequenceClassification.from_config(config)
    model.resize_token_embeddings(len(tokenizer))
    model.load_state_dict(checkpoint['model'], strict=False)
    _ = model.to(device)
    nlp = pipeline('text-classification', model=model, tokenizer=tokenizer)
    return nlp

def get_relation_information(nlp, dataframe):
    graphs = {}
    for idx, row in tqdm(dataframe.iterrows(), total=dataframe.shape[0]):
        try:
            result = nlp(row['text'])
            try:
                graphs[row['PMCID']].append({'left-node-id': row['left-ids'], 'left-node-type': row['left-type'], 'left-node-text': row['left-label'], 'relation': result[0]['label'], 'right-node-id': row['right-ids'], 'right-node-type': row['right-type'], 'right-node-text': row['right-label']})
            except:
                graphs[row['PMCID']] = [{'left-node-id': row['left-ids'], 'left-node-type': row['left-type'], 'left-node-text': row['left-label'], 'relation': result[0]['label'], 'right-node-id': row['right-ids'], 'right-node-type': row['right-type'], 'right-node-text': row['right-label']}]
        except:
            pass
    return graphs