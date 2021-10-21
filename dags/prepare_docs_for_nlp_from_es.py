""" Get all doc ids from an ES index and triggers NLP preprocessing for each Doc ID
"""

from airflow.decorators import dag
from airflow.utils.dates import days_ago
from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.pool import CreatePoolOperator

from tasks.helpers import (
    dag_param_to_dict,
    get_params,
    get_item,
)
from lib.pool import url_to_pool

from normalizers.elastic_settings import settings
from normalizers.elastic_mapping import mapping
from tasks.elastic import create_index, handle_all_ids

# import json
# from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {"owner": "airflow"}

default_dag_params = {
    "item": "http://www.eea.europa.eu/api/@search?portal_type=Highlight&sort_order=reverse&sort_on=Date&created.query=2021/6/1&created.range=min&b_size=500",
    "params": {
        "portal_type": "",
        "elastic": {
            "host": "elastic",
            "port": 9200,
            "index": "data_raw",
            "mapping": mapping,
            "settings": settings,
            "target_index": "data_nlp",
        },
        "rabbitmq": {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "queue_nlp",
        },
        "nlp": {
            "services": {
                "embedding": {
                    "host": "nlp-searchlib",
                    "port": "8000",
                    "path": "api/embedding",
                }
            },
            "text": {
                "props": ["description", "key_message", "summary", "text"],
                "blacklist": ["contact", "rights"],
                "split_length": 500,
            },
        },
        "url_api_part": "api/SITE",
    },
}


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

    create_index(xc_params, add_embedding=True)

    xc_pool_name = url_to_pool(xc_item, prefix="prepare_doc_for_nlp")
    cpo = CreatePoolOperator(
        task_id="create_pool", name=xc_pool_name, slots=16
    )

    handle_all_ids(xc_dag_params, xc_pool_name, "prepare_doc_for_nlp")


prepare_docs_for_nlp_from_es_dag = prepare_docs_for_nlp_from_es()
