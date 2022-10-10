from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from crawlers.registry import get_site_crawler, get_doc_crawler

from lib.dagrun import trigger_dag
from lib import rabbitmq
from lib.pool import val_to_pool, simple_val_to_pool

import logging

logger = logging.getLogger(__file__)

from tasks.helpers import dag_param_to_dict, load_variables, get_params

# from tasks.elastic import create_raw_index
# from tasks.pool import CreatePoolOperator
# from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.pool import CreatePoolOperator
from tasks.debug import debug_value

default_args = {"owner": "airflow"}


default_dag_params = {"params": {"site": "sdi", "fast": True}}


def send_to_rabbitmq(v, raw_doc):
    print("send_to_rabbitmq:")
    print(raw_doc)
    rabbitmq_config = v.get("rabbitmq")
    rabbitmq.send_to_rabbitmq(raw_doc, rabbitmq_config)


def doc_handler(v, site, site_config, doc_id, handler=None):
    pool_name = simple_val_to_pool(site, "crawl_with_query")
    task_params = {
        "item": doc_id,
        "params": {
            "site": site,
            "variables": v,
            "enable_prepare_docs": v.get("enable_prepare_docs", False),
        },
    }

    trigger_dag("d3_crawl_fetch_for_id", task_params, pool_name)


@task
def get_site(task_params):
    print("Site:")
    print(task_params["site"])
    return task_params["site"]


@task
def parse_all_documents(task_params, pool_name):
    print(task_params)
    task_params["variables"]["enable_prepare_docs"] = task_params.get(
        "enable_prepare_docs", False
    )
    site_id = task_params["site"]
    site_config_v = task_params["variables"]["Sites"][site_id]
    site_config = task_params["variables"][site_config_v]
    crawl_type = site_config.get("type", "plone_rest_api")
    parse_all_documents = get_site_crawler(crawl_type)
    handler = get_doc_crawler(crawl_type)

    if not task_params.get("fast", None):
        handler = doc_handler

    # def crawl_doc(v, site, sdi_conf, metadataIdentifier, handler=None):
    # def crawl_doc(v, site, site_config, doc_id, handler=None):
    parse_all_documents(
        task_params["variables"],
        site_id,
        site_config,
        handler,
        send_to_rabbitmq,
    )


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    description="Entrypoint for crawling",
    tags=["crawl"],
)
def d2_crawl_site(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)
    xc_site_id = get_site(xc_params)
    xc_pool_name = val_to_pool(xc_site_id, prefix="crawl_with_query")
    cpo = CreatePoolOperator(task_id="create_pool", name=xc_pool_name, slots=4)
    pd = parse_all_documents(xc_params, xc_pool_name)

    cpo >> pd


crawl_dag = d2_crawl_site()