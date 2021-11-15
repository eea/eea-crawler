"""
"""
from datetime import datetime
import json
import magic
import requests
from urllib.parse import urlparse

from tenacity import retry, wait_exponential, stop_after_attempt

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import trafilatura

from tasks.helpers import simple_dag_param_to_dict
from tasks.rabbitmq import simple_send_to_rabbitmq  # , send_to_rabbitmq
from lib.debug import pretty_id

from datetime import timedelta
from normalizers.elastic_settings import settings
from normalizers.elastic_mapping import mapping
from lib.pool import create_pool
from lib.dagrun import trigger_dag
from lib.pool import simple_url_to_pool

default_dag_params = {
    "item": "https://www.eea.europa.eu/highlights/better-raw-material-sourcing-can",
    "params": {
        "trigger_searchui": False,
        "trigger_nlp": False,
        "rabbitmq": {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "queue_raw_data",
            "nlp_queue": "queue_nlp",
            "searchui_queue": "queue_searchui",
        },
        "url_api_part": "api/SITE",
    },
}

default_args = {"owner": "airflow"}


@task()
def get_api_url(url, params):
    no_protocol_url = url.split("://")[-1]
    return simple_get_api_url(no_protocol_url, params)


def simple_get_api_url(url, params):
    if params["url_api_part"] in url:
        print(url)
        return url
    url_parts = url.split("/")
    if "://" in url:
        url_parts.insert(3, params["url_api_part"])
    else:
        url_parts.insert(1, params["url_api_part"])
    url_with_api = "/".join(url_parts)
    return url_with_api


@task
def add_id(doc, item):
    return simple_add_id(doc, item)


def simple_add_id(doc, item):
    data = json.loads(doc)
    data["id"] = item
    return data


def simple_remove_api_url(url, params):
    return "/".join(url.split("/" + params["url_api_part"] + "/"))


def simple_add_about(doc, value):
    doc["about"] = value
    return doc


def doc_to_raw(doc, web_text):
    raw_doc = {}
    raw_doc["id"] = doc["id"]
    raw_doc["@type"] = doc["@type"]
    raw_doc["raw_value"] = json.dumps(doc)
    if web_text:
        raw_doc["web_text"] = json.dumps(web_text)
    return raw_doc


@retry(wait=wait_exponential(), stop=stop_after_attempt(5))
def request_with_retry(url):
    print("check")
    print(url)
    r = requests.get(url, headers={"Accept": "application/json"})
    print("response")
    print(r.text)
    # test if response is json
    json.loads(r.text)
    print("it's ok")
    return r.text


@retry(wait=wait_exponential(), stop=stop_after_attempt(5))
def trafilatura_with_retry(url):
    print("trafilatura:")
    print(url)
    r = requests.post(
        "http://headless-chrome-api:3000/content",
        headers={"Content-Type": "application/json"},
        data=f'{{"url":"{url}", "js":true,"raw":true}}',
    )
    downloaded = r.text
    # downloaded = trafilatura.fetch_url(url)
    print(downloaded)
    if magic.from_buffer(downloaded) == "data":
        return None
    return trafilatura.extract(downloaded)


@task
def fetch_and_send_to_rabbitmq(full_config):
    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)
    url_with_api = simple_get_api_url(dag_params["item"], dag_params["params"])
    r = request_with_retry(url_with_api)
    doc = simple_add_id(r, dag_params["item"])
    url_without_api = simple_remove_api_url(url_with_api, dag_params["params"])
    web_text = ""
    if dag_params["params"].get("scrape_pages", False):
        web_text = trafilatura_with_retry(url_without_api)

    doc = simple_add_about(doc, url_without_api)
    raw_doc = doc_to_raw(doc, web_text)
    raw_doc["modified"] = doc.get(
        "modified", doc.get("modification_date", None)
    )
    raw_doc["site"] = urlparse(url_without_api).netloc
    raw_doc["indexed_at"] = datetime.now().isoformat()
    simple_send_to_rabbitmq(raw_doc, dag_params["params"])
    if dag_params["params"].get("trigger_searchui", False):
        pool_name = simple_url_to_pool(
            url_with_api, prefix="prepare_doc_for_searchui"
        )
        print("searchui enabled")
        create_pool(pool_name, 16)
        trigger_dag_id = "facets_2_prepare_doc_for_search_ui"
        dag_params["params"]["raw_doc"] = doc
        dag_params["params"]["web_text"] = web_text
        dag_params["params"]["rabbitmq"]["queue"] = dag_params["params"][
            "rabbitmq"
        ]["searchui_queue"]
        trigger_dag(trigger_dag_id, dag_params, pool_name)

    if dag_params["params"].get("trigger_nlp", False):
        pool_name = simple_url_to_pool(
            url_with_api, prefix="prepare_doc_for_nlp"
        )
        print("nlp enabled")
        create_pool(pool_name, 16)
        trigger_dag_id = "nlp_2_prepare_doc_for_nlp"
        dag_params["params"]["raw_doc"] = doc
        dag_params["params"]["web_text"] = web_text
        dag_params["params"]["rabbitmq"]["queue"] = dag_params["params"][
            "rabbitmq"
        ]["nlp_queue"]
        trigger_dag(trigger_dag_id, dag_params, pool_name)


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=[
        "optional: trigger facets_2_prepare_doc_for_search_ui and nlp_2_prepare_doc_for_nlp"
    ],
    description="Get document from plone rest api, optional: scrape the url, optional: trigger facets_2_prepare_doc_for_search_ui and nlp_2_prepare_doc_for_nlp",
)
def raw_3_fetch_url_raw(item=default_dag_params):
    """
    ### get info about an url
    """
    fetch_and_send_to_rabbitmq(item)


fetch_url_raw_dag = raw_3_fetch_url_raw()
