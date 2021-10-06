import json
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
)
from lib.pool import url_to_pool
from normalizers.elastic_settings import settings
from normalizers.elastic_mapping import mapping
from tasks.elastic import simple_create_index
from normalizers.defaults import normalizers

default_args = {"owner": "airflow"}


default_dag_params = {
    "item": "http://www.eea.europa.eu/api/@search?b_size=10&sort_order=reverse&sort_on=Date&portal_type=Highlight",
    "params": {
        "trigger_searchui": True,
        "trigger_nlp": True,
        "rabbitmq": {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "queue_raw_data",
            "searchui_queue": "queue_searchui",
            "nlp_queue": "queue_nlp",
        },
        "elastic": {
            "host": "elastic",
            "port": 9200,
            "mapping": mapping,
            "settings": settings,
            "searchui_target_index": "data_searchui",
            "nlp_target_index": "data_nlp",
        },
        "url_api_part": "api/SITE",
        "nlp": {
            "services": {
                "embedding": {
                    "host": "nlp-searchlib",
                    "port": "8000",
                    "path": "api/embedding",
                }
            },
            "text": {
                "props": ["description", "key_message", "summary", "text"],
                "blacklist": ["contact", "rights"],
                "split_length": 500,
            },
        },
        "normalizers": normalizers,
    },
}


@task
def get_no_protocol_url(url: str):
    return url.split("://")[-1]


@task()
def extract_docs_from_json(page):
    json_doc = json.loads(page)
    docs = json_doc["items"]
    urls = [doc["@id"] for doc in docs]
    print("Number of documents:")
    print(len(urls))
    return urls


@task()
def check_trigger_searchui(params):
    if params.get("trigger_searchui", False):
        params["elastic"]["target_index"] = params["elastic"][
            "searchui_target_index"
        ]
        simple_create_index(params)
    return params


@task()
def check_trigger_nlp(params):
    if params.get("trigger_nlp", False):
        params["elastic"]["target_index"] = params["elastic"][
            "nlp_target_index"
        ]
        simple_create_index(params, add_embedding=True)
    return params


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def crawl_with_query(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = check_trigger_searchui(xc_params)
    xc_params = check_trigger_nlp(xc_params)
    xc_item = get_item(xc_dag_params)

    xc_endpoint = get_no_protocol_url(xc_item)

    debug_value(xc_dag_params)
    page = SimpleHttpOperator(
        task_id="get_docs_request",
        method="GET",
        endpoint=xc_endpoint,
        headers={"Accept": "application/json"},
    )
    xc_urls = extract_docs_from_json(page.output)

    xc_items = build_items_list(xc_urls, xc_params)

    xc_pool_name = url_to_pool(xc_item, prefix="fetch_url_raw")

    cpo = CreatePoolOperator(task_id="create_pool", name=xc_pool_name, slots=4)

    bt = BulkTriggerDagRunOperator(
        task_id="fetch_url_raw",
        items=xc_items,
        trigger_dag_id="fetch_url_raw",
        custom_pool=xc_pool_name,
    )


crawl_with_query_dag = crawl_with_query()
