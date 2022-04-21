import json
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from tenacity import retry, stop_after_attempt, wait_exponential
import logging

logger = logging.getLogger(__file__)

from lib.variables import get_variable
from tasks.helpers import (
    dag_param_to_dict,
    simple_dag_param_to_dict,
    load_variables,
    build_items_list,
    get_params,
)
from tasks.rabbitmq import simple_send_to_rabbitmq
from tasks.elastic import create_raw_index
from tasks.pool import CreatePoolOperator
from tasks.dagrun import BulkTriggerDagRunOperator

default_args = {"owner": "airflow"}


default_dag_params = {"params": {"site": "sdi"}}


@retry(wait=wait_exponential(), stop=stop_after_attempt(1))
def request_with_retry(url, method="get", data=None, authorization=None):
    logger.info("Fetching %s", url)

    handler = getattr(requests, method)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if authorization:
        headers["authorization"] = authorization
    resp = handler(
        url,
        headers=headers,
        data=json.dumps(data)
        #'{"query": {"query_string": {"query": "+resourceType:series"}}, "track_total_hits": true}'
    )
    logger.info("Response: %s", resp.text)

    assert json.loads(resp.text)  # test if response is json
    logger.info("Response is valid json")
    print(resp.text)

    return json.loads(resp.text)


@task
def get_docs_from_sdi(task_params):
    site_config_variable = get_variable("Sites").get(task_params["site"], None)
    site_config = get_variable(site_config_variable)
    val = request_with_retry(
        site_config["endpoint"],
        "post",
        site_config["query"],
        site_config.get("authorization", None),
    )

    return [
        src["_source"]["metadataIdentifier"] for src in val["hits"]["hits"]
    ]


@task
def parse_hits(hits, full_config):
    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)
    dag_variables = dag_params["params"].get("variables", {})
    rabbitmq_config = get_variable("rabbitmq", dag_variables)

    for hit in hits:
        doc = hit["_source"]
        raw_doc = {}
        raw_doc["id"] = doc.get("metadataIdentifier", "")
        raw_doc["@type"] = "series"
        raw_doc["raw_value"] = doc
        raw_doc["site_id"] = "sdi"
        raw_doc["modified"] = doc.get("changeDate", None)
        simple_send_to_rabbitmq(raw_doc, rabbitmq_config)


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    description="Entrypoint for crawling sdi",
    tags=["sdi"],
)
def sdi_1_crawl(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)
    xc_docs = get_docs_from_sdi(xc_params)
    xc_items = build_items_list(xc_docs, xc_params)
    cri = create_raw_index()

    cpo = CreatePoolOperator(task_id="create_pool", name="sdi", slots=4)

    bt = BulkTriggerDagRunOperator(
        task_id="sdi_2_fetch_for_id",
        items=xc_items,
        trigger_dag_id="sdi_2_fetch_for_id",
        custom_pool=cpo.output,
    )

    # ph = parse_hits(xc_es_hits, xc_dag_params)
    cri >> bt


crawl_sdi_dag = sdi_1_crawl()
