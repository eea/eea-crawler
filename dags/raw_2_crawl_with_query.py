import json
from urllib import robotparser
from urllib.parse import urlparse

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator

from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.pool import CreatePoolOperator

from tasks.debug import debug_value
from tasks.helpers import (
    dag_param_to_dict,
    build_items_list,
    get_params,
    get_item,
    get_es_variable,
    set_attr,
    find_site_by_url,
    load_variables,
)
from lib.pool import val_to_pool  # url_to_pool,
from lib.variables import get_variable

from tasks.elastic import simple_create_index, create_raw_index

# get_attr,
# from airflow.operators.python_operator import BranchPythonOperator
# from normalizers.elastic_settings import settings
# from normalizers.elastic_mapping import mapping

default_args = {"owner": "airflow"}

URL = (
    "http://www.eea.europa.eu/api/@search?b_size=10&metadata_fields=modified&"
    "show_inactive=true&sort_order=reverse&sort_on=Date&portal_type=Highlight"
)

default_dag_params = {
    "item": URL,
    "params": {"site": "eea"},
}


@task
def get_no_protocol_url(url: str):
    return url.split("://")[-1]


@task
def extract_docs_from_json(page, params):
    site_config_variable = get_variable("Sites").get(params["site"], None)
    site_config = get_variable(site_config_variable)
    types_blacklist = site_config.get("types_blacklist", [])
    print("TYPES BLACKLIST:")
    print(types_blacklist)
    json_doc = json.loads(page)
    print("EXTRACT_DOCS_FROM_JSON")
    print(json_doc)
    docs = json_doc["items"]
    urls = [doc["@id"] for doc in docs if doc["@type"] not in types_blacklist]
    print("Number of documents:")
    print(len(urls))

    for item in urls:
        print(item)

    return urls


@task
def extract_next_from_json(page, params):
    site_config_variable = get_variable("Sites").get(params["site"], None)
    site_config = get_variable(site_config_variable)

    if params.get("trigger_next_bulk", False):
        json_doc = json.loads(page)
        if json_doc.get("batching", {}).get("next", False):
            next_url = json_doc.get("batching", {}).get("next")
            if site_config.get("fix_items_url", None):
                if site_config["fix_items_url"]["without_api"] in next_url:
                    next_url = next_url.replace(
                        site_config["fix_items_url"]["without_api"],
                        site_config["fix_items_url"]["with_api"],
                    )
            print("NEXT URL:")
            print(next_url)
            return [next_url]

    return []


@task
def check_trigger_searchui(params, es):
    print(es)
    if params.get("trigger_searchui", False):
        es["target_index"] = es["searchui_target_index"]
        simple_create_index(es)
    return params


@task
def check_trigger_nlp(params, es):
    if params.get("trigger_nlp", False):
        es["target_index"] = es["nlp_target_index"]
        simple_create_index(es, add_embedding=True)
    return params


def remove_api_url(url, params):
    if params.get("fix_items_url", None):
        if params["fix_items_url"]["without_api"] in url:
            return url
        if params["fix_items_url"]["with_api"] in url:
            return url.replace(
                params["fix_items_url"]["with_api"],
                params["fix_items_url"]["without_api"],
            )
    ret_url = "/".join(url.split("/" + params["url_api_part"] + "/"))

    # Handle languages
    if url.find("www.eea.europa.eu") > -1:
        if url.find("/api/"):
            ret_url = "/".join(ret_url.split("/api/"))

    return ret_url


@task
def check_robots_txt(url, items, params):
    site_config_variable = get_variable("Sites").get(params["site"], None)
    site_config = get_variable(site_config_variable)
    if site_config.get("ignore_robots_txt", False):
        return items

    allowed_items = []
    robots_url = f"{site_config['url']}/robots.txt"
    print(robots_url)
    rp = robotparser.RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    for item in items:
        item_url = remove_api_url(item, site_config)
        if rp.can_fetch("*", item_url):
            allowed_items.append(item)

    return allowed_items


@task
def get_concurrency(params):
    site_config_variable = get_variable("Sites").get(params["site"], None)
    site_config = get_variable(site_config_variable)
    return site_config.get("concurrency", 4)


@task
def find_site(url):
    site = find_site_by_url(url)
    print(site)
    return site


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["trigger raw_3_fetch_url_raw for each document"],
    description="""Query site for a content type, trigger
    raw_3_fetch_url_raw for each document""",
)
def raw_2_crawl_with_query(item=default_dag_params):
    create_raw_index()

    xc_dag_params = dag_param_to_dict(item, default_dag_params)

    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)

    xc_es = get_es_variable("elastic")
    xc_es_mapping = get_es_variable("elastic_mapping")
    xc_es_settings = get_es_variable("elastic_settings")

    xc_es = set_attr(xc_es, "mapping", xc_es_mapping)
    xc_es = set_attr(xc_es, "settings", xc_es_settings)

    xc_params = check_trigger_searchui(xc_params, xc_es)
    xc_params = check_trigger_nlp(xc_params, xc_es)

    xc_item = get_item(xc_dag_params)
    xc_endpoint = get_no_protocol_url(xc_item)

    debug_value(xc_dag_params)
    page = SimpleHttpOperator(
        task_id="get_docs_request",
        method="GET",
        endpoint=xc_endpoint,
        headers={"Accept": "application/json"},
    )
    xc_urls = extract_docs_from_json(page.output, xc_params)

    xc_allowed_urls = check_robots_txt(xc_item, xc_urls, xc_params)

    xc_items = build_items_list(xc_allowed_urls, xc_params)

    xc_site = find_site(xc_item)
    # xc_pool_name = url_to_pool(xc_item, prefix="fetch_url_raw")

    xc_pool_name = val_to_pool(xc_site, prefix="fetch_url_raw")

    xc_concurrency = get_concurrency(xc_params)

    cpo = CreatePoolOperator(
        task_id="create_pool", name=xc_pool_name, slots=xc_concurrency
    )

    bt = BulkTriggerDagRunOperator(
        task_id="fetch_url_raw",
        items=xc_items,
        trigger_dag_id="raw_3_fetch_url_raw",
        custom_pool=xc_pool_name,
    )

    xc_next = extract_next_from_json(page.output, xc_params)
    debug_value(xc_next)

    # xc_pool_name_next = url_to_pool(xc_item, prefix="crawl_with_query")
    xc_pool_name_next = val_to_pool(xc_site, prefix="crawl_with_query")
    cpo_next = CreatePoolOperator(
        task_id="create_pool_next", name=xc_pool_name_next, slots=1
    )

    xc_items_next = build_items_list(xc_next, xc_params)

    bt = BulkTriggerDagRunOperator(
        task_id="crawl_with_query",
        items=xc_items_next,
        trigger_dag_id="raw_2_crawl_with_query",
        custom_pool=xc_pool_name_next,
    )


crawl_with_query_dag = raw_2_crawl_with_query()
