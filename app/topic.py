import gensim
import pandas
import operator
import re

from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from gensim import corpora, models

from keybert import KeyBERT
from tqdm import tqdm

def run_topic_and_keyphrase_extraction(collectionname):
    kw_model = KeyBERT(model='all-mpnet-base-v2')
    lda_model = gensim.models.LdaMulticore.load('resource/lda_model')
    dictionary = gensim.corpora.Dictionary.load('resource/lda_dictionary')
    
    dataframe = pandas.read_csv('tsv/downloaded.tsv', sep='\t', index_col=0)
    pmcids = dataframe['fileid'].values.tolist()
    pmcids = list(set(pmcids))
    
    for pmcid in tqdm(pmcids):
        text = dataframe[(dataframe['fileid'] == pmcid) & (dataframe['sectype'].isin(['TITLE', 'ABSTRACT']))]['text'].values.tolist()
        keywords = kw_model.extract_keywords(' '.join(text), keyphrase_ngram_range=(1, 3), stop_words='english', highlight=False, use_mmr=True, diversity=0.3, top_n=30)
        keywords = dict(keywords)
        keyphrase = dict(sorted(keywords.items(), key=operator.itemgetter(1), reverse=True)[:10])
        keyphrase = [k for k in keyphrase]
        bow_corpus = [dictionary.doc2bow(doc) for doc in [keywords]]
        
        topic = {}
        for index, score in sorted(lda_model[bow_corpus[0]], key=lambda tup: -1*tup[1]):
            scores = "\nScore: {}\t \nTopic: {}".format(score, lda_model.print_topic(index, 10))
            topicterms = re.findall(r'\"([^\"]+)\"', scores)
            topic['|'.join(topicterms)] = str(score)
        
        for obj in collectionname.find({'PMCID': str(pmcid)}):
            try:
                _ = collectionname.update_one({'_id': obj['_id']}, {'$set': {'KEYPHRASE': keyphrase, 'TOPICS': topic}}, upsert=False)
            except:
                pass