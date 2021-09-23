import json
from elasticsearch import Elasticsearch

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.pool import CreatePoolOperator

from tasks.debug import debug_value
from tasks.helpers import dag_param_to_dict,simple_dag_param_to_dict, build_items_list, get_params, get_item
from tasks.rabbitmq import simple_send_to_rabbitmq, send_to_rabbitmq

from lib.pool import url_to_pool
from lib.debug import pretty_id

from normalizers.defaults import normalizers
from normalizers.normalizers import create_doc, apply_black_map, apply_white_map, remove_empty, apply_norm_obj, apply_norm_prop, apply_norm_missing, remove_duplicates, get_attrs_to_delete, delete_attrs

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
            "queue":"queue_searchui"
        },
        'normalizers': normalizers,
        'url_api_part': 'api/SITE'
    }
}

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
    return normalized_doc

@task
def get_doc_from_raw_idx(item, config):
    return simple_get_doc_from_raw_idx(item, config)

def simple_get_doc_from_raw_idx(item, config):
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

@task
def transform_doc(full_config):
    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)
    doc = simple_get_doc_from_raw_idx(dag_params['item'], dag_params['params'])
    normalized_doc = simple_normalize_doc(doc, dag_params['params'])
    simple_send_to_rabbitmq(normalized_doc, dag_params['params'])

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def prepare_doc_for_search_ui(item = default_dag_params):
    transform_doc(item)
    # xc_dag_params = dag_param_to_dict(item, default_dag_params)
    # debug_value(xc_dag_params)
    # xc_params = get_params(xc_dag_params)
    # xc_item = get_item(xc_dag_params)

    # xc_doc = get_doc_from_raw_idx(xc_item, xc_params)
    # #debug_value(xc_doc)
    # xc_normalized_doc = normalize_doc(xc_doc, xc_params)
    # #debug_value(xc_normalized_doc)


prepare_doc_for_search_ui_dag = prepare_doc_for_search_ui()
