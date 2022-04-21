"""
"""

import json
import logging
from datetime import datetime
from urllib.parse import urlunsplit  # urlparse,

import magic
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from tenacity import retry, stop_after_attempt, wait_exponential

from lib.dagrun import trigger_dag
from lib.pool import create_pool
from lib.variables import get_variable
from normalizers.lib.trafilatura_extract import get_text_from_html
from tasks.elastic import simple_create_raw_index
from tasks.helpers import find_site_by_url, simple_dag_param_to_dict
from tasks.rabbitmq import simple_send_to_rabbitmq  # , send_to_rabbitmq

logger = logging.getLogger(__file__)

# from lib.pool import simple_val_to_pool
# from lib.debug import pretty_id
# from datetime import timedelta


default_dag_params = {
    "item": "ffaf904f-db07-4605-bc79-914b70150f25",
    "params": {
        "site": "sdi",
        "trigger_searchui": False,
        "trigger_nlp": False,
        "create_index": False,
    },
}

default_args = {"owner": "airflow"}


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


def get_doc_from_sdi(doc_id, site_id):
    print("DOC")
    print(doc_id)
    site_config_variable = get_variable("Sites").get(site_id, None)
    site_config = get_variable(site_config_variable)
    val = request_with_retry(
        site_config["endpoint"],
        "post",
        json.loads(
            json.dumps(site_config["metadata_query"]).replace(
                "<metadataIdentifier>", doc_id
            )
        ),
        site_config.get("authorization", None),
    )

    doc = val["hits"]["hits"][0]["_source"]

    doc["children"] = []
    print(doc.get("agg_associated"))
    for child_id in doc.get("agg_associated", []):
        print("CHILD")
        print(child_id)
        child_doc = request_with_retry(
            site_config["endpoint"],
            "post",
            json.loads(
                json.dumps(site_config["metadata_query"]).replace(
                    "<metadataIdentifier>", child_id
                )
            ),
            site_config.get("authorization", None),
        )
        if len(child_doc["hits"]["hits"]) > 0:
            doc["children"].append(child_doc["hits"]["hits"][0]["_source"])

    return doc


@task
def fetch_and_send_to_rabbitmq(full_config):
    rabbitmq_config = get_variable("rabbitmq")

    site_id = full_config["params"]["site"]
    doc_id = full_config["item"]
    doc = get_doc_from_sdi(doc_id, site_id)
    if doc:
        raw_doc = {}
        raw_doc["id"] = doc.get("metadataIdentifier", "")
        raw_doc["@type"] = "series"
        raw_doc["raw_value"] = doc
        raw_doc["site_id"] = "sdi"
        raw_doc["modified"] = doc.get("changeDate", None)
        simple_send_to_rabbitmq(raw_doc, rabbitmq_config)

    return


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
def sdi_2_fetch_for_id(item=default_dag_params):
    """
        ### get info about an url

    Get document from plone rest api, optional: scrape the url,
        optional: trigger facets_2_prepare_doc_for_search_ui and
        nlp_2_prepare_doc_for_nlp
    """
    fetch_and_send_to_rabbitmq(item)


sdi_fetch_for_id_dag = sdi_2_fetch_for_id()
