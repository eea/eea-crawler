"""
"""

import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from app import sdi_crawl
from tasks.helpers import simple_dag_param_to_dict
from tasks.helpers import (
    dag_param_to_dict,
    load_variables,
    get_params,
    get_item
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

@task 
def crawl_doc(metadataIdentifier, task_params):
    sdi_crawl.crawl_doc(task_params['variables'], metadataIdentifier)

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
def crawl_2_fetch_for_id_dag(item=default_dag_params):
    """
        ### get info about an url

    Get document from plone rest api, optional: scrape the url,
        optional: trigger facets_2_prepare_doc_for_search_ui and
        nlp_2_prepare_doc_for_nlp
    """
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)
    xc_item = get_item(xc_dag_params)
    crawl_doc(xc_item, xc_params)


crawl_fetch_for_id_dag = crawl_2_fetch_for_id_dag()
