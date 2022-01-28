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

URL = (
    "https://www.eea.europa.eu/publications/exploring-the-social-challenges-of"
)

default_dag_params = {
    "item": URL,
    "params": {
        "trigger_searchui": False,
        "trigger_nlp": False,
        "create_index": False,
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

    # Handle languages
    if url.find("www.eea.europa.eu") > -1:
        if url.find("/api/"):
            return url

    if params.get("fix_items_url", None):
        if params["fix_items_url"]["without_api"] in url:
            url = url.replace(
                params["fix_items_url"]["without_api"],
                params["fix_items_url"]["with_api"],
            )
        return url

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
    if params.get("fix_items_url", None):
        if params["fix_items_url"]["with_api"] in url:
            url = url.replace(
                params["fix_items_url"]["with_api"],
                params["fix_items_url"]["without_api"],
            )
        return url

    if params["url_api_part"].strip("/") == "":
        return url
    ret_url = "/".join(url.split("/" + params["url_api_part"] + "/"))

    # Handle languages
    if url.find("www.eea.europa.eu") > -1:
        if url.find("/api/"):
            ret_url = "/".join(ret_url.split("/api/"))

    return ret_url


def _add_about(doc, value):
    doc["about"] = value
    return doc


def doc_to_raw(doc, web_html, pdf_text):
    """A middle-ground representation of docs, for the ES "harvested" index"""

    raw_doc = {}
    raw_doc["id"] = doc.get("id", "")
    raw_doc["@type"] = doc.get("@type", "")
    raw_doc["raw_value"] = doc

    if web_html:
        raw_doc["web_html"] = web_html

    if pdf_text:
        raw_doc["pdf_text"] = pdf_text

    return raw_doc


@retry(wait=wait_exponential(), stop=stop_after_attempt(1))
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
def scrape_with_retry(url, js=False):
    logger.info("Scraping url: %s", url)

    if js:
        resp = requests.post(
            "http://headless-chrome-api:3000/content",
            headers={"Content-Type": "application/json"},
            data=f'{{"url":"{url}", "js":true,"raw":true}}',
        )
        downloaded = resp.text
    else:
        resp = requests.get(url)
        downloaded = resp.text

    if magic.from_buffer(downloaded) == "data":
        return None

    logger.info("Downloaded: %s", downloaded)

    return downloaded


FIELD_MARKERS = {"file": {"content-type", "download", "filename"}}


def is_field_of_type(field, _type):
    if not isinstance(field, dict):
        return False

    if _type not in FIELD_MARKERS:
        return False

    return set(field.keys()).issuperset(FIELD_MARKERS[_type])


def fix_download_url(download_url, source_url):
    if "www.eea.europa.eu" in source_url:
        return download_url.replace("@@download", "at_download")
    return download_url


def extract_attachments(json_doc, url, nlp_service_params):
    params = nlp_service_params["converter"]

    logger.info("Extract attachments %s %s", json_doc, url)

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
            try:
                resp = request_with_retry(
                    converter_dsn, "post", {"url": download_url}
                )
            except Exception:
                logger.exception("failed pdf extraction, retry")
                download_url = value["download"]
                logger.info("Retry with download url: %s", download_url)
                resp = request_with_retry(
                    converter_dsn, "post", {"url": download_url}
                )
            if isinstance(resp, str):
                resp = json.loads(resp)
            for doc in resp["documents"]:
                text_fragments.append(doc["text"].strip())

    text = "\n".join(text_fragments)

    logger.info("Retrieved file content: %r", text)

    return text


@task
def fetch_and_send_to_rabbitmq(full_config):
    doc_errors = []
    errors = []

    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)

    dag_variables = dag_params["params"].get("variables", {})
    logger.info("Dag params %s", dag_params)

    if dag_params["params"].get("create_index", False):
        simple_create_raw_index()

    site = find_site_by_url(dag_params["item"], variables=dag_variables)

    site_config_variable = get_variable("Sites", dag_variables).get(site, None)
    site_config = get_variable(site_config_variable, dag_variables)

    nlp_service_params = get_variable("nlp_services", dag_variables)

    url_with_api = _get_api_url(dag_params["item"], site_config)

    r_url = url_with_api
    r_url = f"{url_with_api}?expand=object_provides"
    if site_config.get("avoid_cache_api", False):
        r_url = f"{r_url}&crawler=true"

    try:
        r = request_with_retry(r_url)
        assert json.loads(r)["@id"]
    except Exception:
        logger.exception("retrieving json from api")
        errors.append("retrieving json from api")
        doc_errors.append("json")
        r = json.dumps({"@id": dag_params["item"]})

    doc = _add_id(r, dag_params["item"])
    url_without_api = _remove_api_url(url_with_api, site_config)

    web_html = ""
    try:
        scrape = False
        s_url = ""
        scrape_with_js = False
        if site_config.get("scrape_pages", False):
            s_url = url_without_api
            scrape_with_js = site_config.get("scrape_with_js", False)
            scrape = True
        scrape_for_types = site_config.get("scrape_for_types", False)
        if scrape_for_types:
            scrape_for_type = scrape_for_types.get(doc.get("@type"), False)
            if scrape_for_type is not False:
                scrape = True
                s_url = url_without_api
                scrape_with_js = scrape_for_type.get("scrape_with_js", False)
        if doc.get("@type") == "File":
            scrape = False
        if scrape:
            if site_config.get("avoid_cache_web", False):
                s_url = f"{url_without_api}?scrape=true"
            web_html = scrape_with_retry(
                s_url,
                scrape_with_js,
            )
    except Exception:
        logger.exception("Error scraping the page")
        errors.append("scraping the page")
        doc_errors.append("web")

    pdf_text = ""

    should_extract_pdf = True
    if site_config.get("pdf_days_limit", 0) > 0:
        current_date = datetime.now()
        logger.info("CHECK DATE")
        mod_date_str = doc.get("modification_date", None)
        if mod_date_str:
            mod_date = datetime.strptime(
                mod_date_str.split("T")[0], "%Y-%m-%d"
            )
            logger.info(current_date)
            logger.info(mod_date)
            diff = current_date - mod_date
            delta = diff.days
            logger.info(delta)
            logger.info(site_config.get("pdf_days_limit"))
            if delta > site_config.get("pdf_days_limit"):
                should_extract_pdf = False

    if should_extract_pdf:
        logger.info("EXTRACT PDF")
        try:
            pdf_text = extract_attachments(doc, r_url, nlp_service_params)
        except Exception:
            logger.exception("Error converting pdf file")
            errors.append("converting pdf file")
            doc_errors.append("pdf")

    doc = _add_about(doc, url_without_api)
    raw_doc = doc_to_raw(doc, web_html, pdf_text)
    raw_doc["original_id"] = url_without_api
    raw_doc["site_id"] = site
    doc["site_id"] = site
    raw_doc["errors"] = doc_errors
    raw_doc["modified"] = doc.get(
        "modified", doc.get("modification_date", None)
    )
    raw_doc["site"] = site_config["url"]
    raw_doc["indexed_at"] = datetime.now().isoformat()

    rabbitmq_config = get_variable("rabbitmq", dag_variables)

    simple_send_to_rabbitmq(raw_doc, rabbitmq_config)

    if dag_params["params"].get("trigger_searchui", False):
        logger.info("searchui enabled")
        create_pool("prepare_doc_for_searchui", 16)
        trigger_dag_id = "facets_2_prepare_doc_for_search_ui"
        dag_params["params"]["raw_doc"] = doc
        dag_params["params"]["web_html"] = web_html
        dag_params["params"]["pdf_text"] = raw_doc.get("pdf_text", "")
        trigger_dag(trigger_dag_id, dag_params, "prepare_doc_for_searchui")

    if dag_params["params"].get("trigger_nlp", False):
        logger.info("nlp enabled")
        create_pool("prepare_doc_for_nlp", 16)
        trigger_dag_id = "nlp_2_prepare_doc_for_nlp"
        dag_params["params"]["raw_doc"] = doc
        dag_params["params"]["web_html"] = web_html
        dag_params["params"]["pdf_text"] = raw_doc.get("pdf_text", "")
        trigger_dag(trigger_dag_id, dag_params, "prepare_doc_for_nlp")

    if errors:
        msg = ", ".join(errors)
        logger.warning(f"Error while {msg}, check the logs above")
        raise Exception(f"WARNING: Error while {msg}")


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
