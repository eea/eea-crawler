""" Given a source ES Doc id, cleanup, preprocess and write the doc in an ES index

(via Logstash)
"""

from elasticsearch import Elasticsearch
import json
from unidecode import unidecode
import re
import requests
from tenacity import retry, wait_exponential, stop_after_attempt


from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from lib.debug import pretty_id

from normalizers.defaults import normalizers
from normalizers.normalizers import (
    create_doc, apply_black_map, apply_white_map,
    remove_empty, apply_norm_obj, apply_norm_prop, apply_norm_missing,
    remove_duplicates, get_attrs_to_delete, delete_attrs)
from tasks.helpers import simple_dag_param_to_dict, merge
from tasks.rabbitmq import simple_send_to_rabbitmq

default_args = {
    "owner": "airflow",
}

default_dag_params = {
    'item': "https://www.eea.europa.eu/api/SITE/highlights/better-raw-material-sourcing-can",
    'params': {
        'elastic': {
            'host': 'elastic',
            'port': 9200,
            'index': 'data_raw'
        },
        'rabbitmq': {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "queue_nlp"
        },
        'nlp':{
            'services':{
                'embedding':{
                    'host':'nlp-embedding',
                    'port':'8000',
                    'path':'api/embedding'
                }
            },
            'text':{
                'props':['description', 'key_message', 'summary', 'text'],
                'blacklist':['contact', 'rights'],
                'split_length':500
            }
        },
        'normalizers': normalizers,
        'url_api_part': 'api/SITE'
    }
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def prepare_doc_for_nlp(item=default_dag_params):
    transform_doc(item)


@task
def normalize_doc(doc, config):
    return simple_normalize_doc(doc, config)


def simple_normalize_doc(doc, config):
    normalizer = config['normalizers']
    normalized_doc = create_doc(doc)

    attrs_to_delete = get_attrs_to_delete(normalized_doc, normalizer)
    normalized_doc = apply_black_map(normalized_doc, normalizer)
    normalized_doc = apply_white_map(normalized_doc, normalizer)
    normalized_doc = remove_empty(normalized_doc)
    normalized_doc = apply_norm_obj(normalized_doc, normalizer)
    normalized_doc = apply_norm_prop(normalized_doc, normalizer)
    normalized_doc = apply_norm_missing(normalized_doc, normalizer)
    normalized_doc = remove_duplicates(normalized_doc)
    normalized_doc = delete_attrs(normalized_doc, attrs_to_delete)
    # normalized_doc = restructure_doc(normalized_doc)

    print(normalized_doc)

    return normalized_doc


def get_doc_from_raw_idx(item, config):
    timeout = 1000
    es = Elasticsearch(
        [
            {
                'host': config['elastic']['host'],
                'port': config['elastic']['port']
            }
        ],
        timeout=timeout
    )
    res = es.get(index=config['elastic']['index'], id=pretty_id(item))
    return res['_source']

def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext


def get_haystack_data(json_doc, config):
    #json_doc = json.loads(doc)
    print('Type:', type(json_doc))
    print('Doc:', json_doc)

    txt_props = config['props']
    txt_props_black = config['blacklist']

    # start text with the document title.
    title = json_doc.get("title", "no title")
    text = title + '.\n\n'

    # get other predefined fields first in the order defined in txt_props param
    for prop in txt_props:
        txt = cleanhtml(json_doc.get(prop, {}).get('data', ''))
        if not txt.endswith('.'):
            txt = txt + '.'
        # avoid redundant text
        if txt not in text:
            text = text + txt + '\n\n'

    # find automatically all props that have text or html in it
    # and append to text if not already there.
    for k, v in json_doc.items():
        if (type(v) is dict and k not in txt_props_black):
            txt = ''
            #print(f'%s is a dict' % k)
            mime_type = json_doc.get(k, {}).get('content-type', '')
            if mime_type == 'text/plain':
                #print('%s is text/plain' % k)
                txt = json_doc.get(k, {}).get('data', '')
            elif mime_type == 'text/html':
                #print('%s is text/html' % k)
                txt = cleanhtml(json_doc.get(k, {}).get('data', ''))
            # avoid redundant text
            if txt and txt not in text:
                if not txt.endswith('.'):
                    txt = txt + '.'
                text = text + '\n\n' + k.upper() + ': ' + txt + '\n\n'

    # TODO: further cleanup of text
    # If you're working with English text and don't have to worry about losing diacritics
    # then maybe you can preprocess your text with unidecode.
    # https://githubmemory.com/repo/explosion/spacy-stanza/issues/68
    # https://towardsdatascience.com/cleaning-preprocessing-text-data-by-building-nlp-pipeline-853148add68a
    text = unidecode(text)

    # metadata
    url = json_doc["@id"]
    uid = json_doc['UID']
    content_type = json_doc["@type"]
    creation_date = json_doc["creation_date"]
    publishing_date = json_doc.get('effectiveDate', '')
    expiration_date = json_doc.get('expirationDate', '')
    review_state = json_doc.get('review_state', '')

    # build haystack dict
    dict_doc = {"text": text,
                "meta": {
                    "name": title,
                    "url": url,
                    "uid": uid,
                    "content_type": content_type,
                    "creation_date": creation_date,
                    "publishing_date": publishing_date,
                    "expiration_date": expiration_date,
                    "review_state": review_state
                }
    }

    return dict_doc

@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def preprocess_split_doc(doc, config):
    from haystack.preprocessor.preprocessor import PreProcessor
    preprocessor = PreProcessor(
        clean_empty_lines=True,
        clean_whitespace=True,
        clean_header_footer=False,
        split_by="word",
        split_length=config['split_length'],
        split_respect_sentence_boundary=True
    )

    docs = preprocessor.process(doc)
    print(doc["meta"]["name"])
    print(f"n_docs_output: {len(docs)}")

    for tmp_doc in docs:
        tmp_doc['id'] = f"{tmp_doc['id']}#{tmp_doc['meta']['_split_id']}"
    
    return docs

@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def add_embeddings_doc(docs, nlp_service):
    #data = {'snippets':[doc['text']], "is_passage": True}

    data = {
        'is_passage': True,
        'snippets': []
    }

    for doc in docs:
        data['snippets'].append(doc['text'])
    
    data = json.dumps(data)
    r = requests.post(f"http://{nlp_service['host']}:{nlp_service['port']}/{nlp_service['path']}", headers={"Accept": "application/json","Content-Type": "application/json"}, data=data)

    embeddings = json.loads(r.text)['embeddings']
    for doc in docs:
        for embedding in embeddings:
            if doc['text'] == embedding['text']:
               doc['text_embedding'] = embedding['embedding'] 
    return docs

@task
def transform_doc(full_config):
    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)
    
    #get a single document from elasticsearch
    doc = get_doc_from_raw_idx(dag_params['item'], dag_params['params'])

    #do the same normalization as we do for searchlib, so we can use the same filters 
    normalized_doc = simple_normalize_doc(doc, dag_params['params'])

    #build the haystack document with text field and meta information
    haystack_data = get_haystack_data(doc, dag_params['params']['nlp']['text'])

    #merge the normalized document and the haystack document
    haystack_data = merge(normalized_doc, haystack_data)

    #split the document
    splitted_docs = preprocess_split_doc(haystack_data, dag_params['params']['nlp']['text'])

    #add the embeddings
    docs_with_embedding = add_embeddings_doc(splitted_docs, dag_params['params']['nlp']['services']['embedding'])

    print(docs_with_embedding)

    for doc in docs_with_embedding:
        simple_send_to_rabbitmq(doc, dag_params['params'])


prepare_doc_for_nlp_dag = prepare_doc_for_nlp()


# @task
# def get_doc_from_raw_idx(item, config):
#     return simple_get_doc_from_raw_idx(item, config)
