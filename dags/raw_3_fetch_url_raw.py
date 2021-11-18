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

from tasks.helpers import simple_dag_param_to_dict, find_site_by_url
from tasks.rabbitmq import simple_send_to_rabbitmq  # , send_to_rabbitmq
from lib.debug import pretty_id

from datetime import timedelta
from lib.pool import create_pool
from lib.dagrun import trigger_dag
from lib.pool import simple_url_to_pool
from airflow.models import Variable

default_dag_params = {
    "item": "https://www.eea.europa.eu/highlights/better-raw-material-sourcing-can",
    "params": {
        "trigger_searchui": False,
        "trigger_nlp": False,
    },
}

default_args = {"owner": "airflow"}


@task()
def get_api_url(url, params):
    no_protocol_url = url.split("://")[-1]
    return simple_get_api_url(no_protocol_url, params)


def simple_get_api_url(url, params):
    if params["url_api_part"].strip("/") == "":
        return url
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
    if params["url_api_part"].strip("/") == "":
        return url
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
def trafilatura_with_retry(url, js=False):
    print("TRAFILATURA")
    print(url)
    if js:
        r = requests.post(
            "http://headless-chrome-api:3000/content",
            headers={"Content-Type": "application/json"},
            data=f'{{"url":"{url}", "js":true,"raw":true}}',
        )
        downloaded = r.text
    else:
        downloaded = trafilatura.fetch_url(url)

    if magic.from_buffer(downloaded) == "data":
        return None
    print(downloaded)
    return trafilatura.extract(downloaded)


@task
def fetch_and_send_to_rabbitmq(full_config):

    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)

    site = find_site_by_url(dag_params["item"])

    site_config_variable = Variable.get("Sites", deserialize_json=True).get(
        site, None
    )
    site_config = Variable.get(site_config_variable, deserialize_json=True)

    url_with_api = simple_get_api_url(dag_params["item"], site_config)

    r_url = url_with_api
    if site_config.get("avoid_cache_api", False):
        r_url = f"{url_with_api}?crawler=true"

    r = request_with_retry(r_url)
    doc = simple_add_id(r, dag_params["item"])
    url_without_api = simple_remove_api_url(url_with_api, site_config)

    web_text = ""

    if site_config.get("scrape_pages", False):
        s_url = url_without_api
        if site_config.get("avoid_cache_web", False):
            s_url = f"{url_without_api}?scrape=true"
        web_text = trafilatura_with_retry(
            s_url,
            site_config.get("scrape_with_js", False),
        )

    doc = simple_add_about(doc, url_without_api)
    raw_doc = doc_to_raw(doc, web_text)
    raw_doc["modified"] = doc.get(
        "modified", doc.get("modification_date", None)
    )
    raw_doc["site"] = urlparse(url_without_api).netloc
    raw_doc["indexed_at"] = datetime.now().isoformat()
    rabbitmq_config = Variable.get("rabbitmq", deserialize_json=True)

    simple_send_to_rabbitmq(raw_doc, rabbitmq_config)

    if dag_params["params"].get("trigger_searchui", False):
        pool_name = simple_url_to_pool(
            url_with_api, prefix="prepare_doc_for_searchui"
        )
        print("searchui enabled")
        create_pool(pool_name, 16)
        trigger_dag_id = "facets_2_prepare_doc_for_search_ui"
        dag_params["params"]["raw_doc"] = doc
        dag_params["params"]["web_text"] = web_text
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
