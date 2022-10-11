"""
"""

import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from lib import rabbitmq
from normalizers import normalizer

from tasks.helpers import (
    dag_param_to_dict,
    load_variables,
    get_params,
    get_item,
)

logger = logging.getLogger(__file__)

# from lib.pool import simple_val_to_pool
# from lib.debug import pretty_id
# from datetime import timedelta


default_dag_params = {
    "item": "dacc1cd8-9f85-4dd3-871b-a7bf551fe03a",
    "params": {
        "site": "sdi",
        "trigger_searchui": False,
        "trigger_nlp": False,
        "create_index": False,
    },
}

default_args = {"owner": "airflow"}


def send_to_rabbitmq(v, doc):
    print("send_to_rabbitmq:")
    print(doc)
    index_name = v.get("elastic", {}).get("searchui_target_index",None)
    if index_name is not None:
      doc['index_name'] = index_name
      rabbitmq_config = v.get("rabbitmq")

      rabbitmq.send_to_rabbitmq(doc, rabbitmq_config)


@task
def preprocess_doc(task_params):
    print(task_params)
    v = task_params.get("params", {}).get("variables", {})
    site_id = task_params.get("params", {}).get("site")
    doc_id = task_params.get("item")
    print(doc_id)
    print(site_id)
    raw_doc = task_params["params"].get("raw_doc", False)
    if not raw_doc:
        raw_doc = normalizer.get_raw_doc_by_id(v, doc_id)
    normalizer.preprocess_doc(v, doc_id, site_id, raw_doc, send_to_rabbitmq)


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=[
        "optional: trigger facets_2_prepare_doc_for_search_ui "
        "and nlp_2_prepare_doc_for_nlp"
    ],
    description="""Get document from plone rest api, optional: scrape the url,
    optional: trigger facets_2_prepare_doc_for_search_ui and
    nlp_2_prepare_doc_for_nlp""",
)
def d5_prepare_doc_for_searchui(item=default_dag_params):
    """
        ### get info about an url

    Get document from plone rest api, optional: scrape the url,
        optional: trigger facets_2_prepare_doc_for_search_ui and
        nlp_2_prepare_doc_for_nlp
    """
    preprocess_doc(item)


prepare_doc_for_searchui_dag = d5_prepare_doc_for_searchui()
