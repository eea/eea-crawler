import json
import requests
from tenacity import retry, wait_exponential, stop_after_attempt

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator

from tasks.debug import debug_value
from tasks.helpers import dag_param_to_dict, simple_dag_param_to_dict, build_items_list, get_params, get_item
from tasks.rabbitmq import simple_send_to_rabbitmq, send_to_rabbitmq
from lib.debug import pretty_id

default_dag_params = {
    'item': "https://www.eea.europa.eu/highlights/better-raw-material-sourcing-can",
#    'item': "https://www.eea.europa.eu/api/SITE/highlights/walking-cycling-and-public-transport",
    'params':{
        'rabbitmq': {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue":"default"
        },
        'url_api_part': 'api/SITE'
    }
}

default_args = {
    "owner": "airflow",
}

@task()
def get_api_url(url, params):
    no_protocol_url = url.split("://")[-1]
    return simple_get_api_url(no_protocol_url, params)

def simple_get_api_url(url, params):
    if params['url_api_part'] in url:
        print(url)
        return url
    url_parts = url.split("/")
    if "://" in url:
        url_parts.insert(3, params['url_api_part'])
    else:
        url_parts.insert(1, params['url_api_part'])
    url_with_api = "/".join(url_parts)
    return url_with_api

@task
def add_id(doc, item):
    return simple_add_id(doc, item)

def simple_add_id(doc, item):
    data = json.loads(doc)
    data["id"] = pretty_id(item)
    return data

def simple_remove_api_url(url, params):
    return "/".join(url.split("/"+params['url_api_part'] + '/'))

def simple_add_about(doc, value):
    doc["about"] = value
    return doc

@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def request_with_retry(url):
    r = requests.get(url, headers={"Accept": "application/json"})
    return r.text

@task
def fetch_and_send_to_rabbitmq(full_config):
    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)
    url_with_api = simple_get_api_url(dag_params['item'], dag_params['params'])
    r = request_with_retry(url_with_api)
    doc = simple_add_id(r, dag_params['item'])
    url_without_api = simple_remove_api_url(url_with_api,dag_params['params'])

    doc = simple_add_about(doc, url_without_api)
    simple_send_to_rabbitmq(doc, dag_params['params'])

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def fetch_url_raw(item = default_dag_params):
    """
    ### get info about an url
    """
    fetch_and_send_to_rabbitmq(item)

    # xc_dag_params = dag_param_to_dict(item, default_dag_params)

    # xc_params = get_params(xc_dag_params)
    # xc_item = get_item(xc_dag_params)

    # xc_url_with_api = get_api_url(xc_item, xc_params)

    # doc = SimpleHttpOperator(
    #     task_id="get_doc",
    #     method="GET",
    #     endpoint=xc_url_with_api,
    #     headers={"Accept": "application/json"},
    # )

    # xc_doc=add_id(doc.output, xc_item)
    # send_to_rabbitmq(xc_doc, xc_params)

fetch_url_raw_dag = fetch_url_raw()
