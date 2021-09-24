import json
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.pool import CreatePoolOperator

from tasks.debug import debug_value
from tasks.helpers import (
    dag_param_to_dict, build_items_list, get_params, get_item)
from lib.pool import url_to_pool

default_args = {
    "owner": "airflow",
}

default_dag_params = {
    'item': "http://www.eea.europa.eu/api/@search?portal_type=Highlight&sort_order=reverse&sort_on=Date&created.query=2019/6/1&created.range=min&b_size=500",
    'params': {
        'rabbitmq': {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "queue_raw_data"
        },
        'url_api_part': 'api/SITE'
    }
}


@task
def get_no_protocol_url(url: str):
    return(url.split("://")[-1])


@task()
def extract_docs_from_json(page):
    json_doc = json.loads(page)
    docs = json_doc['items']
    urls = [doc["@id"] for doc in docs]
    return urls


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def crawl_with_query(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)

    xc_params = get_params(xc_dag_params)
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

    xc_pool_name = url_to_pool(xc_item)

    cpo = CreatePoolOperator(
        task_id="create_pool",
        name=xc_pool_name,
        slots=2,
    )

    bt = BulkTriggerDagRunOperator(
        task_id="fetch_url_raw",
        items=xc_items,
        trigger_dag_id="fetch_url_raw",
        custom_pool=xc_pool_name,
    )


crawl_with_query_dag = crawl_with_query()
