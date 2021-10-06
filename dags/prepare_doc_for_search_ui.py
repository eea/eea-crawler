""" Given a source ES Doc id, cleanup and write the doc in an ES index

(via Logstash)
"""
import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from normalizers.defaults import normalizers
from normalizers.normalizers import simple_normalize_doc
from tasks.helpers import simple_dag_param_to_dict
from tasks.rabbitmq import simple_send_to_rabbitmq
from tasks.elastic import (get_doc_from_raw_idx)

default_args = {"owner": "airflow"}

default_dag_params = {
    "item": "https://www.eea.europa.eu/api/SITE/highlights/better-raw-material-sourcing-can",
    "params": {
        "elastic": {"host": "elastic", "port": 9200, "index": "data_raw"},
        "rabbitmq": {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "queue_searchui",
        },
        "nlp": {
            "text": {
                "blacklist": ["contact", "rights"],
            },
        },
        "normalizers": normalizers,
        "url_api_part": "api/SITE",
    },
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def prepare_doc_for_search_ui(item=default_dag_params):
    transform_doc(item)


@task
def normalize_doc(doc, config):
    return simple_normalize_doc(doc, config)


@task
def transform_doc(full_config):
    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)
    if dag_params["params"].get("raw_doc", None):
        doc = dag_params["params"].get("raw_doc")
    else:
        doc = get_doc_from_raw_idx(dag_params["item"], dag_params["params"])
    normalized_doc = simple_normalize_doc(doc, dag_params["params"])
    simple_send_to_rabbitmq(normalized_doc, dag_params["params"])


prepare_doc_for_search_ui_dag = prepare_doc_for_search_ui()


# @task
# def get_doc_from_raw_idx(item, config):
#     return simple_get_doc_from_raw_idx(item, config)
