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
    set_attr,
)
from tasks.elastic import simple_create_index
from lib.pool import val_to_pool
from lib.variables import get_variable


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {"owner": "airflow"}
default_dag_params = {
    "item": "ias",
    "params": {
        "query_size": 500,
        "trigger_next_bulk": True,
        "trigger_nlp": False,
        "trigger_searchui": False,
    },
}


@task
def build_queries_list(config):
    url = config["site"]["url"].strip("/")
    if config["site"].get("fix_items_url", None):
        if config["site"]["fix_items_url"]["without_api"] in url:
            url = url.replace(
                config["site"]["fix_items_url"]["without_api"],
                config["site"]["fix_items_url"]["with_api"],
            )
    else:
        url_api_part = config["site"]["url_api_part"].strip("/")
        if url_api_part != "":
            url = f"{url}/{url_api_part}"

    if config["site"].get("portal_types", None):
        # queries = []
        queries = [
            f"{url}/@search?b_size={config['params']['query_size']}&metadata_fields=modified&show_inactive=true&sort_order=reverse&sort_on=Date&portal_type={portal_type}"
            for portal_type in config["site"]["portal_types"]
        ]
        if config["site"].get("languages", None):
            for language in config["site"].get("languages"):
                queries.append(
                    f"{url}/{language}/@search?b_size={config['params']['query_size']}&metadata_fields=modified&show_inactive=true&sort_order=reverse&sort_on=Date"
                )
    else:
        queries = [
            f"{url}/@search?b_size={config['params']['query_size']}&metadata_fields=modified&show_inactive=true&sort_order=reverse&sort_on=Date"
        ]
    print(queries)
    return queries


@task
def get_site_config(task_params):
    params = task_params
    site = params.get("item", None)
    config = {}
    sites = get_variable("Sites")
    site_config_variable = sites.get(site, None)
    if site_config_variable:
        site_config = get_variable(site_config_variable)
        normalizers_config_variable = site_config.get(
            "normalizers_variable", "default_normalizers"
        )
        normalizers_config = get_variable(normalizers_config_variable)
        config["site"] = site_config
        if params["params"].get("portal_types", None):
            config["site"]["portal_types"] = params["params"]["portal_types"]
        if params["params"].get("languages", None):
            config["site"]["languages"] = params["params"]["languages"]
        config["normalizers"] = normalizers_config

        config["nlp_services"] = get_variable("nlp_services")
        config["elastic"] = get_variable("nlp_services")
        config["rabbitmq"] = get_variable("rabbitmq")
        config["params"] = params["params"]
        config["params"]["site"] = site
    return config


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["trigger raw_2_crawl_with_query for each type"],
    description="Entrypoint for a site, accepts a list of types, and triggers raw_2_crawl_with_query for each type",
)
def raw_1_crawl_plonerestapi_website(item=default_dag_params):
    """
    ### Crawls a plone.restapi powered website.

    Main task to crawl a website
    """

    # get_variables()
    xc_dag_params = dag_param_to_dict(item, default_dag_params)

    xc_site_config = get_site_config(xc_dag_params)

    xc_item = get_item(xc_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = set_attr(xc_params, "site", xc_item)

    xc_queries = build_queries_list(xc_site_config)

    xc_items = build_items_list(xc_queries, xc_params)

    xc_pool_name = val_to_pool(xc_item, prefix="crawl_with_query")

    cpo = CreatePoolOperator(task_id="create_pool", name=xc_pool_name, slots=1)

    bt = BulkTriggerDagRunOperator(
        task_id="crawl_with_query",
        items=xc_items,
        trigger_dag_id="raw_2_crawl_with_query",
        custom_pool=xc_pool_name,
    )


crawl_website_dag = raw_1_crawl_plonerestapi_website()
