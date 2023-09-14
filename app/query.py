import re
import pandas
import requests
import os

import xml.etree.ElementTree as ET

from tqdm import tqdm
from datetime import datetime
from nltk.tokenize import sent_tokenize

def get_sentences(text, candidates):
    sentences = []
    text = text.split('\n')
    for t in text:
        tmp = []
        for sent in sent_tokenize(t):
            if (len(tmp) >= 1):
                check = False
                for c in candidates:
                    if bool(re.match(r'.*'+re.escape(c)+r'[\)\]\-\"]*$', tmp[-1])):                
                        check = True
                        break
                for c in ['i.p.', 'i.v.', 'mol.', 'wt.', 'sp.', 'spp.', 'spec.']:
                    if (tmp[-1].endswith(c)):
                        check = True
                if bool(re.match(r'^[^A-Za-z]+$', sent)) | bool(re.match(r'^[^\[\(\{]+[\]\)\}].*$', sent)):
                    check = True
                if (check):
                    tmp[-1] += ' ' + sent
                else:
                    if (len(re.findall(r'\([^\)\(]+\)\.[A-Z]', sent)) > 0):
                        sent = re.sub(r'(\([^\)\(]+\)\.)([A-Z])', r'\1---\2', sent)
                        sent = re.split('---', sent)
                        tmp.extend(sent)
                    elif (len(re.findall(r'[a-z0-9\)\"]+\.[\(\"]*[A-Z]', sent)) > 0):
                        sent = re.sub(r'([a-z0-9\)\"]+\.)([\(\"]*[A-Z])', r'\1---\2', sent)
                        sent = re.split('---', sent)
                        tmp.extend(sent)
                    else:
                        tmp.append(sent)
            else:
                tmp.append(sent)
        tmp = [re.sub(r'\s+', ' ', t) for t in tmp]
        tmp = [t for t in tmp if (not t.strip() == '')]
        for sent in tmp:
            if (not sent.strip() == ''):
                sentences.append(sent)
    return sentences

def get_based_on_query(lastupdate, file):
    pmcids = []
    queries = []
    with open(file, 'r') as f:
        queries = f.readlines()
        queries = [q.strip() for q in queries]
    try:
        for query in tqdm(queries):
            url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pmc&term={0}+AND+free+fulltext[filter]&mindate={1}&retmax=10000'.format(query, lastupdate)
            result = requests.get(url)
            if (result.status_code == 200):
                tree = ET.ElementTree(ET.fromstring(result.text))
                for x in tree.find('IdList'):
                    pmcids.append(x.text)
        
        if (len(pmcids) > 0):
            return ['Success', pmcids]
        else:
            return ['No new record found!', pmcids]
    except:
        return ['Failed', pmcids]

def get_based_on_identifier(inp):
    errormessage = 'Unidentified'
    try:
        url = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pmc&id={0}'.format(inp)
        result = requests.get(url)
        if (result.status_code == 200):
            tree = ET.ElementTree(ET.fromstring(result.text))
            with open('xml/{0}.xml'.format(inp), 'wb') as f:
                _ = tree.write(f)
            return ['Success', inp]
    except:
        return ['Retrieval Failed', errormessage]

def get_label_section_pubmed():
    labelsorted = {}
    with open('resource/Structured-Abstracts-Labels-102615.txt', 'r') as f:
        lines = f.readlines()
    
    for line in lines:
        line = line.strip()
        if (not line == ''):
            line = line.split('|')
            _key = re.sub(r'[^A-Z]+', ' ', line[0].upper())
            _key = re.sub(r'\s+', ' ', _key)
            labelsorted[_key.strip()] = line[1]
    
    return labelsorted

def get_body_text(obj):
    symbols = {'&lt;': '<', '&gt;': '>', '&amp;': '&', '&apos;': "'", '&quot;': '"'}
    txt = ET.tostring(obj, encoding='utf-8').decode('utf-8')
    txt = txt.strip()
    txt = re.sub(r'\<[^\<\>]+\/\>', '', txt)
    txt = re.sub(r'\<italic[^\<\>]*\>[ ]*\<\/italic\>', '', txt)
    
    while(bool(re.search(r'\<\/*[^\<\>]+\/*\>', txt))):
        for quote in re.findall(r'\<\/*[^\<\>]+\/*\>', txt):
            if (not quote.startswith('<italic')) & (not quote.startswith('</italic')):
                txt = re.sub(re.escape(quote), '', txt)
        quotes = [quote for quote in re.findall(r'\<\/*[^\<\>]+\/*\>', txt)]
        quotes = [q for q in quotes if (not q.startswith('<italic')) & (not q.startswith('</italic'))]
        if (quotes == []):
            break
    
    txt = re.sub(r'\<italic[^\<\>]*\>[ ]*\<\/italic\>', '', txt)
    italics = []
    while(bool(re.search(r'\<\/*[^\<\>]+\/*\>', txt))):
        for quote in re.findall(r'\<italic[^\<\>]*\>([^\<\>]+)\<\/italic\>', txt):
            italics.append(quote)
        txt = re.sub(r'\<italic[^\<\>]*\>([^\<\>]+)\<\/italic\>', r'\1', txt)
    
    italics = [i for i in italics if (not i.strip() == '')]
    italics = [re.sub(r'\&amp\;', '&', t) for t in italics]
    italics = [re.sub(r'\&amp', '&', t) for t in italics]    
    txt = re.sub(r'\&amp\;', '&', txt)
    txt = re.sub(r'\&amp', '&', txt)
    for sym in symbols:
        txt = re.sub(re.escape(sym), symbols[sym], txt)
        italics = [re.sub(re.escape(sym), symbols[sym], t) for t in italics]    
    italics = list(set(italics))
    _ = italics.sort()
    return txt, italics

def post_pmc_document_mongo(inp, collectionname, labelsorted):
    ids = {}
    journal = {}
    pubdate = {}
    title = []
    abstract = {}
    body = {}
    keywords = {}
    italics = []
    
    tree = ET.parse('xml/{0}.xml'.format(inp))
    root = tree.getroot()
    
    article = root.find('article')
    for leaf in article:
        if (leaf.tag.endswith('front')):
            pid = 0
            for meta in leaf:
                if (meta.tag.endswith('journal-meta')):
                    for metainf in meta:
                        if (metainf.tag.endswith('journal-id')):
                            journal[metainf.get('journal-id-type')] = metainf.text
                        elif (metainf.tag.endswith('issn')):
                            journal[metainf.get('pub-type')] = metainf.text
                        elif (metainf.tag.endswith('journal-title-group')):
                            for inf in metainf:
                                if (inf.tag.endswith('journal-title')):
                                    journal['title'] = inf.text
                elif (meta.tag.endswith('article-meta')):
                    for metainf in meta:
                        if (metainf.tag.endswith('article-id')):
                            ids[metainf.get('pub-id-type')] = metainf.text
                        elif (metainf.tag.endswith('title-group')):
                            for _title in metainf:
                                if (_title.tag.endswith('article-title')):
                                    txt, _ = get_body_text(_title)
                                    title.append(txt)
                        elif (metainf.tag.endswith('pub-date')):
                            _pubdate = ['0'] * 8
                            for _date in metainf:
                                if (_date.tag.endswith('day')):
                                    _pubdate[6:8] = _date.text.zfill(2)
                                elif (_date.tag.endswith('month')):
                                    if (len(_date.text) > 2):
                                        _pubdate[4:6] = dictmonth[_date.text]
                                    else:
                                        _pubdate[4:6] = _date.text.zfill(2)
                                elif (_date.tag.endswith('year')):
                                    _pubdate[0:4] = _date.text
                            if (bool(metainf.get('pub-type'))):
                                pubdate[metainf.get('pub-type')] = ''.join(_pubdate)
                        elif (metainf.tag.endswith('abstract')):
                            parent_map = dict((c, p) for p in list(metainf.iter()) for c in p if (c.tag == 'p') | (c.tag == 'sec'))
                            for p in metainf.iter('p'):
                                label = []
                                parent = parent_map[p]
                                if (parent.tag.endswith('sec')) | (parent.tag.endswith('abstract')):
                                    for part in parent:
                                        if (part.tag.endswith('title')):
                                            if (part.text):
                                                labelname = re.sub(r'[^\x00-\x7f]', r'', part.text)
                                                label.append(labelname.upper())
                                txt, _italic = get_body_text(p)
                                if (label == []):
                                    label = ['ABSTRACT']
                                if (not txt == ''):
                                    abstract['ABS{0}'.format(str(pid))] = tuple([txt, 'ABSTRACT', str(label)])
                                    pid += 1
                                if (not _italic == []):
                                    italics.append({'ABS{0}'.format(str(pid)): _italic})
                        elif (metainf.tag.endswith('kwd-group')):
                            group = 'keywords'
                            _keywords = []
                            for _kwd in metainf:
                                if (_kwd.tag == 'title'):
                                    if (bool(_kwd.text)):
                                        group = _kwd.text
                                elif (_kwd.tag.endswith('kwd')):
                                    txt, _ = get_body_text(_kwd)
                                    _keywords.append(txt)
                            keywords[group] = _keywords
        elif (leaf.tag.endswith('body')):
            parent_map = dict((c, p) for p in list(leaf.iter()) for c in p)
            pid = 0
            paragraphs = [p for p in leaf.iter('p')]
            last_sec_type = ''
            for p in paragraphs:
                label = []
                parent = parent_map[p]
                if (parent.tag.endswith('caption')) | (parent.tag.endswith('fig')) | ('table' in parent.tag):
                    label.append('[UNUSED]')
                
                for part in parent:
                    if (part.tag.endswith('title')):
                        if (part.text):
                            labelname = re.sub(r'[^\x00-\x7f]', r'', part.text)
                            label.append(labelname.upper())
                
                sectype = ''
                while (parent in parent_map):
                    grandparent = parent_map[parent]
                    if (grandparent.tag.endswith('caption')) | (grandparent.tag.endswith('fig')) | ('table' in grandparent.tag):
                        label.append('[UNUSED]')
                    for part in grandparent:
                        if (part.tag.endswith('title')):
                            if (part.text):
                                labelname = re.sub(r'[^\x00-\x7f]', r'', part.text)
                                label.append(labelname.upper())
                    if (grandparent.get('sec-type')):
                        sectype = grandparent.get('sec-type')
                    parent = grandparent
                    
                if (not '[UNUSED]' in label):
                    if (not sectype == ''):
                        sectype = sectype.split('|')[0]
                        sectype = re.sub(r'[^A-Z]+', ' ', sectype.upper())
                        sectype = re.sub(r'\s+', ' ', sectype)
                        if (sectype.strip() in labelsorted):
                            sectype = labelsorted[sectype.strip()]
                        else:
                            sectype = ''
                    
                    if (sectype == '') & (not label == []):
                        tmplabel = str(label).upper()
                        if (not bool(re.match(r'.*(\bFUNDING.*\b|\bAUTHOR.*\b).*', tmplabel))) & (not bool(re.match(r'.*(\bCOMPETING\b|\bDECLARATION.*\s+OF\b|\bCONFLICT.*\s+OF\b).*INTEREST.*', tmplabel))) & (not bool(re.match(r'.*(\bCOMPUTER\s+CODE\b|\bDATA\b).*AVAILABILITY.*', tmplabel))) & (not bool(re.match(r'.*\bETHIC.*\bSTATEMENT.*', tmplabel))) & (not bool(re.match(r'.*\bSUPPLEMENTARY\b.*(MATERIAL*|DATA*|TABLE*).*', tmplabel))):
                            label_0 = label[-1].upper() 
                            label_0 = re.sub(r'[^A-Z]+', ' ', label_0)
                            label_0 = re.sub(r'\s+', ' ', label_0)
                            label_0 = label_0.strip()
                            if (label_0 in labelsorted):
                                sectype = labelsorted[label_0]
                                last_sec_type = sectype
                            elif (pid-1 > -1):
                                sectype = last_sec_type
                            else:
                                sectype = 'BACKGROUND'
                                last_sec_type = sectype
                        else:
                            sectype = 'UNUSED'                            
                    elif (sectype == '') & (label == []):
                        if (pid == 0):
                            sectype = 'BACKGROUND'
                        else:
                            sectype = body['BODY' + str(pid-1)][1]
                else:
                    sectype = 'UNUSED'
                
                txt, _italic = get_body_text(p)
                label = label[::-1]
                label = [l for l in label if (not l == '[UNUSED]')]
                if (not txt == ''):
                    body['BODY{0}'.format(str(pid))] = tuple([txt, sectype, label])
                    pid += 1
                if (not _italic == []):
                    italics.append({'BODY{0}'.format(str(pid)): _italic})
    
    obj = {}
    obj['created-date'] = datetime.now().strftime('%Y%m%d %H:%M:%s')
    obj['updated-date'] = datetime.now().strftime('%Y%m%d %H:%M:%s')
    obj['PMCID'] = inp
    obj['PMCURL'] = 'https://www.ncbi.nlm.nih.gov/pmc/articles/{0}/'.format(inp)
    obj['OTHER-IDS'] = ids
    obj['JOURNAL'] = journal
    obj['PUBDATE'] = pubdate
    obj['TITLE'] = title
    obj['ABSTRACT'] = {}
    for a in abstract:
        obj['ABSTRACT'][a] = abstract[a][0]
    
    obj['BODY'] = {}
    for b in body:
        if (not body[b][1] in obj['BODY']):
            obj['BODY'][body[b][1]] = {}
        obj['BODY'][body[b][1]][b] = {'LABELS': body[b][2], 'TEXT': body[b][0]}
    
    obj['KEYWORDS'] = keywords
    obj['SENTENCES'] = []
    obj['RELATIONS'] = []
    obj['TOPICS'] = {}
    obj['KEYPHRASE'] = []
    obj['ITALICS'] = italics
    if (not obj['TITLE'] == []):
        try:
            _ = collectionname.insert_one(obj)
            return ["Success", title, abstract, body]
        except:
            return ["Failed"]
    else:
        return ["Failed"]