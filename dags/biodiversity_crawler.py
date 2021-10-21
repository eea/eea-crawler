""" Given a website URL, it reads sitemap and triggers fetch url on each link in the
sitemap
"""

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

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

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {"owner": "airflow"}
default_dag_params = {
    "item": "https://biodiversity.europa.eu",
    "params": {
        "query_size": 500,
        "trigger_next_bulk": True,
        "trigger_nlp": False,
        "trigger_searchui": False,
        "url_api_part": "api",
    },
}


@task
def build_queries_list(url, params):
    print(url)
    print(params)
    queries = [
        f"{url}/api/@search?b_size={params['query_size']}&metadata_fields=modified&show_inactive=true&sort_order=reverse&sort_on=Date"
    ]
    print(queries)
    return queries


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def biodiversity_crawler(item=default_dag_params):
    """
    ### Crawls a plone.restapi powered website.

    Main task to crawl a website
    """
    xc_dag_params = dag_param_to_dict(item, default_dag_params)

    xc_params = get_params(xc_dag_params)
    xc_item = get_item(xc_dag_params)

    xc_queries = build_queries_list(xc_item, xc_params)

    xc_items = build_items_list(xc_queries, xc_params)
    xc_pool_name = url_to_pool(xc_item, prefix="crawl_with_query")

    cpo = CreatePoolOperator(task_id="create_pool", name=xc_pool_name, slots=1)

    bt = BulkTriggerDagRunOperator(
        task_id="crawl_with_query",
        items=xc_items,
        trigger_dag_id="crawl_with_query",
        custom_pool=xc_pool_name,
    )


biodiversity_crawler_dag = biodiversity_crawler()
