"""
"""

import logging
from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from crawlers.registry import get_site_crawler, get_doc_crawler
from lib import rabbitmq

from tasks.helpers import simple_dag_param_to_dict
from tasks.helpers import (
    dag_param_to_dict,
    load_variables,
    get_params,
    get_item,
)
from tasks.debug import debug_value
from lib.pool import create_pool
from lib.dagrun import trigger_dag

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


def send_to_rabbitmq(v, raw_doc):
    print("send_to_rabbitmq:")
    print(raw_doc)
    rabbitmq_config = v.get("rabbitmq")
    rabbitmq.send_to_rabbitmq(raw_doc, rabbitmq_config)
    errors = raw_doc.get("errors", [])
    if len(errors) > 0:
        msg = ", ".join(errors)
        logger.warning(f"Error while {msg}, check the logs above")

        raise Exception(f"WARNING: Error while {msg}")


@task
def crawl_doc(task_params):
    v = task_params.get("params", {}).get("variables", {})
    site_id = task_params.get("params", {}).get("site")
    doc_id = task_params.get("item")

    print(doc_id)
    print(task_params)
    site_config_v = v["Sites"][site_id]
    site_config = v[site_config_v]
    crawl_type = site_config.get("type", "plone_rest_api")
    crawler = get_doc_crawler(crawl_type)

    doc = crawler(v, site_id, site_config, doc_id, send_to_rabbitmq)

    print("check preprocess")
    if task_params["params"].get("enable_prepare_docs", False):
        print("preprocess enabled")
        if len(doc.get("errors")) == 0:
            print("there were no errors at crawling, we can preprocess")
            logger.info("preprocess enabled")
            create_pool("prepare_for_searchui", 16)
            task_params_prepare = {
                "item": doc_id,
                "params": {
                    "raw_doc": doc.get("raw_doc"),
                    "site": site_id,
                    "variables": v,
                },
            }
            trigger_dag(
                "d5_prepare_doc_for_searchui",
                task_params_prepare,
                "prepare_for_searchui",
            )


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
def d3_crawl_fetch_for_id(item=default_dag_params):
    """
        ### get info about an url

    Get document from plone rest api, optional: scrape the url,
        optional: trigger facets_2_prepare_doc_for_search_ui and
        nlp_2_prepare_doc_for_nlp
    """
    crawl_doc(item)


crawl_fetch_for_id_dag = d3_crawl_fetch_for_id()
