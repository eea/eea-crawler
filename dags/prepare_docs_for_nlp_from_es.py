""" Get all doc ids from an ES index and triggers NLP preprocessing for each Doc ID
"""

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.pool import CreatePoolOperator

from tasks.debug import debug_value
from tasks.helpers import (
    dag_param_to_dict, build_items_list, get_params, get_item)
from lib.pool import url_to_pool
from elasticsearch import Elasticsearch

from normalizers.elastic_settings import settings
from normalizers.elastic_mapping import mapping

# import json
# from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
    "owner": "airflow",
}

default_dag_params = {
    'item': "http://www.eea.europa.eu/api/@search?portal_type=Highlight&sort_order=reverse&sort_on=Date&created.query=2021/6/1&created.range=min&b_size=500",
    'params': {
        'elastic': {
            'host': 'elastic',
            'port': 9200,
            'index': 'data_raw',
            'mapping': mapping,
            'settings': settings,
            'target_index': 'data_nlp'
        },
        'rabbitmq': {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "queue_nlp"
        },
        'url_api_part': 'api/SITE'
    }
}


@task
def get_all_ids(config):
    timeout = 1000
    size = 1000
    body = {}

    # Init Elasticsearch instance
    es = Elasticsearch(
        [
            {
                'host': config['elastic']['host'],
                'port': config['elastic']['port']
            }
        ],
        timeout=timeout
    )

    ids = []
    # Process hits here

    def process_hits(hits):
        for item in hits:
            # print(json.dumps(item, indent=2))
            ids.append(item["_id"])

    # Check index exists
    if not es.indices.exists(index=config['elastic']['index']):
        print("Index " + config['elastic']['index'] + " not exists")
        exit()

    # Init scroll by search
    data = es.search(
        index=config['elastic']['index'],
        scroll='2m',
        size=size,
        body=body,
        _source=["@id"],
    )

    # Get the scroll ID
    sid = data['_scroll_id']
    scroll_size = len(data['hits']['hits'])

    while scroll_size > 0:
        "Scrolling..."

        # Before scroll, process current batch of hits
        process_hits(data['hits']['hits'])
        data = es.scroll(scroll_id=sid, scroll='2m')

        # Update the scroll ID
        sid = data['_scroll_id']

        # Get the number of results that returned in the last scroll
        scroll_size = len(data['hits']['hits'])

    return ids


@task
def create_index(config):
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
    # body = {"settings":config['elastic']['settings']}
    body = {
        "mappings": {
            "properties": config['elastic']['mapping']
        },
        "settings": config['elastic']['settings']
    }

    es.indices.create(index=config['elastic']['target_index'], body=body)


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def prepare_docs_for_nlp_from_es(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)

    xc_params = get_params(xc_dag_params)
    xc_item = get_item(xc_dag_params)

    create_index(xc_params)

    xc_ids = get_all_ids(xc_params)
    debug_value(xc_ids)

    xc_items = build_items_list(xc_ids, xc_params)

    xc_pool_name = url_to_pool(xc_item)

    cpo = CreatePoolOperator(
        task_id="create_pool",
        name=xc_pool_name,
        slots=16,
    )

    bt = BulkTriggerDagRunOperator(
        task_id="prepare_doc_for_nlp",
        items=xc_items,
        trigger_dag_id="prepare_doc_for_nlp",
        custom_pool=xc_pool_name,
    )


prepare_docs_for_nlp_from_es_dag = prepare_docs_for_nlp_from_es()
