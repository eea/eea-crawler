from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from usp.tree import sitemap_tree_for_homepage
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

default_args = {"owner": "airflow"}

default_dag_params = {
    "item": "http://eea.europa.eu",
    "params": {
        "rabbitmq": {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue": "default",
        },
        "url_api_part": "api/SITE",
    },
}


@task
def get_sitemap(item):
    tree = sitemap_tree_for_homepage(item)
    urls = []
    for page in tree.all_pages():
        urls.append(page.url)
    return urls


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def crawl_with_sitemap(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)

    xc_params = get_params(xc_dag_params)
    xc_item = get_item(xc_dag_params)

    debug_value(xc_dag_params)

    xc_urls = get_sitemap(xc_item)
    debug_value(xc_urls)
    xc_items = build_items_list(xc_urls, xc_params)
    debug_value(xc_items)

    xc_pool_name = url_to_pool(xc_item, prefix="fetch_url_raw")

    cpo = CreatePoolOperator(task_id="create_pool", name=xc_pool_name, slots=8)

    bt = BulkTriggerDagRunOperator(
        task_id="fetch_url_raw",
        items=xc_items,
        trigger_dag_id="fetch_url_raw",
        custom_pool=xc_pool_name,
    )


crawl_with_sitemap_dag = crawl_with_sitemap()
