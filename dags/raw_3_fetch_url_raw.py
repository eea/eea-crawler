"""
"""

from datetime import datetime
import json
import magic
import requests
from urllib.parse import urlparse, urlunsplit

from tenacity import retry, wait_exponential, stop_after_attempt

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import trafilatura

from tasks.helpers import simple_dag_param_to_dict, find_site_by_url
from tasks.rabbitmq import simple_send_to_rabbitmq  # , send_to_rabbitmq

from lib.pool import create_pool
from lib.dagrun import trigger_dag
from lib.pool import simple_url_to_pool
from airflow.models import Variable
import logging

logger = logging.getLogger(__file__)

# from lib.debug import pretty_id
# from datetime import timedelta

URL = (
    "https://www.eea.europa.eu/publications/exploring-the-social-challenges-of"
)

default_dag_params = {
    "item": URL,
    "params": {
        "trigger_searchui": False,
        "trigger_nlp": False,
    },
}

default_args = {"owner": "airflow"}


@task()
def get_api_url(url, params):
    no_protocol_url = url.split("://")[-1]
    return _get_api_url(no_protocol_url, params)


def _get_api_url(url, params):
    """Convert an URL to a plone.restapi compatible endpoint

    Params.url_api_part can be something like:

    - `/`
    - `/api`
    - `https://water.europa.eu/api`

    """

    if params["url_api_part"].strip("/") == "":
        return url

    if params["url_api_part"] in url:
        logger.info(
            "Found url_api_part (%s) in url: %s", params["url_api_part"], url
        )
        return url

    url_parts = url.split("/")
    if "://" in url:
        url_parts.insert(3, params["url_api_part"])
    else:
        url_parts.insert(1, params["url_api_part"])

    return "/".join(url_parts)


@task
def add_id(doc, item):
    return _add_id(doc, item)


def _add_id(doc, item):
    data = json.loads(doc)
    data["id"] = item
    return data


def _remove_api_url(url, params):
    if params["url_api_part"].strip("/") == "":
        return url
    return "/".join(url.split("/" + params["url_api_part"] + "/"))


def _add_about(doc, value):
    doc["about"] = value
    return doc


def doc_to_raw(doc, web_text):
    """A middle-ground representation of docs, for the ES "harvested" index"""

    raw_doc = {}
    raw_doc["id"] = doc["id"]
    raw_doc["@type"] = doc["@type"]
    raw_doc["raw_value"] = json.dumps(doc)

    if web_text:
        raw_doc["web_text"] = json.dumps(web_text)

    return raw_doc


@retry(wait=wait_exponential(), stop=stop_after_attempt(5))
def request_with_retry(url, method="get", data=None):
    logger.info("Fetching %s", url)

    handler = getattr(requests, method)
    resp = handler(
        url, headers={"Accept": "application/json"}, data=json.dumps(data)
    )
    logger.info("Response: %s", resp.text)

    assert json.loads(resp.text)  # test if response is json
    logger.info("Response is valid json")

    return resp.text


@retry(wait=wait_exponential(), stop=stop_after_attempt(5))
def trafilatura_with_retry(url, js=False):
    logger.info("Fetching with trafilatura: %s", url)

    if js:
        resp = requests.post(
            "http://headless-chrome-api:3000/content",
            headers={"Content-Type": "application/json"},
            data=f'{{"url":"{url}", "js":true,"raw":true}}',
        )
        downloaded = resp.text
    else:
        downloaded = trafilatura.fetch_url(url)

    if magic.from_buffer(downloaded) == "data":
        return None

    logger.info("Downloaded with trafilatura: %s", downloaded)

    return trafilatura.extract(downloaded)


FIELD_MARKERS = {"file": {"content-type", "download", "filename"}}


def is_field_of_type(field, _type):
    if not isinstance(field, dict):
        return False

    if _type not in FIELD_MARKERS:
        return False

    return set(field.keys()).issuperset(FIELD_MARKERS[_type])


def fix_download_url(download_url, source_url):
    if "eea.europa.eu" in source_url:
        return download_url.replace("@@download", "at_download")
    return download_url


def extract_attachments(doc, url, nlp_service_params):
    params = nlp_service_params["converter"]

    logger.info("Extract attachments %s %s", doc, url)
    json_doc = json.loads(doc["raw_value"])

    converter_dsn = urlunsplit(
        (
            "http",
            params["host"] + ":" + params["port"],
            params["path"],
            "",
            "",
        )
    )

    text_fragments = []

    for name, value in json_doc.items():
        if (
            is_field_of_type(value, "file")
            and value["content-type"] == "application/pdf"
        ):
            download_url = fix_download_url(value["download"], url)
            logger.info("Download url found: %s", download_url)
            resp = request_with_retry(
                converter_dsn, "post", {"url": download_url}
            )
            if isinstance(resp, str):
                resp = json.loads(resp)
            for doc in resp["documents"]:
                text_fragments.append(doc["text"].strip())

    text = "\n".join(text_fragments)

    logger.info("Retrieved file content: %r", text)

    doc["text"] += text

    return doc


@task
def fetch_and_send_to_rabbitmq(full_config):

    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)

    logger.info("Dag params %s", dag_params)

    site = find_site_by_url(dag_params["item"])

    site_config_variable = Variable.get("Sites", deserialize_json=True).get(
        site, None
    )
    site_config = Variable.get(site_config_variable, deserialize_json=True)

    nlp_service_params = Variable.get("nlp_services", deserialize_json=True)

    url_with_api = _get_api_url(dag_params["item"], site_config)

    r_url = url_with_api
    if site_config.get("avoid_cache_api", False):
        r_url = f"{url_with_api}?crawler=true"

    r = request_with_retry(r_url)
    doc = _add_id(r, dag_params["item"])
    url_without_api = _remove_api_url(url_with_api, site_config)

    web_text = ""

    if site_config.get("scrape_pages", False):
        s_url = url_without_api
        if site_config.get("avoid_cache_web", False):
            s_url = f"{url_without_api}?scrape=true"
        web_text = trafilatura_with_retry(
            s_url,
            site_config.get("scrape_with_js", False),
        )

    doc = _add_about(doc, url_without_api)
    raw_doc = doc_to_raw(doc, web_text)
    raw_doc["modified"] = doc.get(
        "modified", doc.get("modification_date", None)
    )
    raw_doc["site"] = urlparse(url_without_api).netloc
    raw_doc["indexed_at"] = datetime.now().isoformat()

    extract_attachments(raw_doc, r_url, nlp_service_params)

    rabbitmq_config = Variable.get("rabbitmq", deserialize_json=True)

    simple_send_to_rabbitmq(raw_doc, rabbitmq_config)

    if dag_params["params"].get("trigger_searchui", False):
        pool_name = simple_url_to_pool(
            url_with_api, prefix="prepare_doc_for_searchui"
        )
        logger.info("searchui enabled")
        create_pool(pool_name, 16)
        trigger_dag_id = "facets_2_prepare_doc_for_search_ui"
        dag_params["params"]["raw_doc"] = doc
        dag_params["params"]["web_text"] = web_text
        trigger_dag(trigger_dag_id, dag_params, pool_name)

    if dag_params["params"].get("trigger_nlp", False):
        pool_name = simple_url_to_pool(
            url_with_api, prefix="prepare_doc_for_nlp"
        )
        logger.info("nlp enabled")
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
        "optional: trigger facets_2_prepare_doc_for_search_ui "
        "and nlp_2_prepare_doc_for_nlp"
    ],
    description="""Get document from plone rest api, optional: scrape the url,
    optional: trigger facets_2_prepare_doc_for_search_ui and
    nlp_2_prepare_doc_for_nlp""",
)
def raw_3_fetch_url_raw(item=default_dag_params):
    """
        ### get info about an url

    Get document from plone rest api, optional: scrape the url,
        optional: trigger facets_2_prepare_doc_for_search_ui and
        nlp_2_prepare_doc_for_nlp
    """
    fetch_and_send_to_rabbitmq(item)


fetch_url_raw_dag = raw_3_fetch_url_raw()