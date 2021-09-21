import json
from elasticsearch import Elasticsearch

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.pool import CreatePoolOperator

from tasks.debug import debug_value
from tasks.helpers import dag_param_to_dict, build_items_list, get_params, get_item
from lib.pool import url_to_pool
from lib.debug import pretty_id

from normalizers.normalizers import normalizers

default_args = {
    "owner": "airflow",
}

default_dag_params = {
    'item': "https://www.eea.europa.eu/api/SITE/highlights/better-raw-material-sourcing-can", 
    'params':{
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
            "queue":"searchui_queue"
        },
        'normalizers': normalizers,
        'url_api_part': 'api/SITE'
    }
}

def simplify_elements(element, element_key):
    clean_element = {}
    if isinstance(element, dict):
        for key in element.keys():
            new_elements = simplify_elements(element[key], key)
            for new_key in new_elements.keys():

                new_element_key = new_key
                if len(element_key) > 0:
                    new_element_key = element_key + "." + new_key
                clean_element[new_element_key] = new_elements[new_key]
    else:
      clean_element[element_key] = element
    return clean_element

def create_doc(doc):
    return simplify_elements(doc, '')

def apply_black_map(doc, config):
    black_map = config['blackMap']
    clean_data = {}
    for key in doc.keys():
        value = None
        if black_map.get(key, None) is None:
            value = doc[key]
        else:
            if isinstance(doc[key], list):
                tmp_value = []
                for val in doc[key]:
                    if val not in black_map[key]:
                        tmp_value.append(val)
                value = tmp_value
            else:
                if doc[key] in black_map[key]:
                    value = None
        clean_data[key] = value
    return clean_data

def apply_white_map(doc, config):
    white_map = config['whiteMap']
    clean_data = {}
    for key in doc.keys():
        value = None
        if white_map.get(key, None) is None:
            value = doc[key]
        else:
            if isinstance(doc[key], list):
                tmp_value = []
                for val in doc[key]:
                    if val in white_map[key]:
                        tmp_value.append(val)
                value = tmp_value
            else:
                if doc[key] in white_map[key]:
                    value = doc[key]
        clean_data[key] = value
    return clean_data

def remove_empty(doc):
    clean_data = {}
    for key in doc.keys():
        ignore_attr = False
        if isinstance(doc[key], list):
            if len(doc[key]) == 0:
                ignore_attr = True
        else:
            if doc[key] is None:
                ignore_attr = True
            else:
                 if isinstance(doc[key], str) and len(doc[key]) == 0:
                    ignore_attr = True
        if not ignore_attr:
            clean_data[key] = doc[key]
    return clean_data

def apply_norm_obj(doc, config):
    return doc
def apply_norm_prop(doc, config):
    return doc
def apply_norm_missing(doc, config):
    return doc
def remove_duplicates(doc):
    return doc

@task 
def normalize_doc(doc, config):
    normalizer = config['normalizers']
    normalized_doc = create_doc(doc)
    normalized_doc = apply_black_map(normalized_doc, normalizer)
    normalized_doc = apply_white_map(normalized_doc, normalizer)
    normalized_doc = remove_empty(normalized_doc)
    normalized_doc = apply_norm_obj(normalized_doc, normalizer)
    normalized_doc = apply_norm_prop(normalized_doc, normalizer)
    normalized_doc = apply_norm_missing(normalized_doc, normalizer)
    normalized_doc = remove_duplicates(normalized_doc)
    return normalized_doc

@task
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

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def preprare_doc_for_search_ui(item = default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)

    xc_params = get_params(xc_dag_params)
    xc_item = get_item(xc_dag_params)

    xc_doc = get_doc_from_raw_idx(xc_item, xc_params)
    debug_value(xc_doc)
    xc_normalized_doc = normalize_doc(xc_doc, xc_params)
    debug_value(xc_normalized_doc)


reprare_doc_for_search_ui_dag = preprare_doc_for_search_ui()
