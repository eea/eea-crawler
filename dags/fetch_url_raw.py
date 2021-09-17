import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from eea.rabbitmq.client import RabbitMQConnector

from tasks.debug import debug_value
from tasks.helpers import dag_param_to_dict, build_items_list, get_params, get_item
from lib.debug import pretty_id

default_dag_params = {
    'item': "page_url", 
    'params':{
        'rabbitmq': {
            "host": "rabbitmq",
            "port": "5672",
            "username": "guest",
            "password": "guest",
            "queue":"default"
        },
        'url_api_part': 'api'
    }
}

default_args = {
    "owner": "airflow",
}

@task
def send_to_rabbitmq(doc, params):
    rabbit_config = {
        "rabbit_host": params['rabbitmq']['host'],
        "rabbit_port": params['rabbitmq']['port'],
        "rabbit_username": params['rabbitmq']['username'],
        "rabbit_password": params['rabbitmq']['password'],
    }
    params['rabbitmq']
    queue_name = params['rabbitmq']['queue']

    rabbit = RabbitMQConnector(**rabbit_config)
    rabbit.open_connection()
    rabbit.declare_queue(queue_name)
    rabbit.send_message(queue_name, json.dumps(doc))
    rabbit.close_connection()

@task()
def get_api_url(url, params):
    no_protocol_url = url.split("://")[-1]
    if '/api/' in no_protocol_url:
        print(no_protocol_url)
        return no_protocol_url
    url_parts = no_protocol_url.split("/")
    url_parts.insert(1, params['url_api_part'])
    url_with_api = "/".join(url_parts)
    print(url_with_api)
    return url_with_api

@task
def add_id(doc, item):
    data = json.loads(doc)
    data["id"] = pretty_id(item)
    return data

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
    xc_dag_params = dag_param_to_dict(item, default_dag_params)

    xc_params = get_params(xc_dag_params)
    xc_item = get_item(xc_dag_params)

    xc_url_with_api = get_api_url(xc_item, xc_params)

    doc = SimpleHttpOperator(
        task_id="get_doc",
        method="GET",
        endpoint=xc_url_with_api,
        headers={"Accept": "application/json"},
    )

#    debug_value(xc_doc.output)
    # prepared_data = get_relevant_data(doc.output, item)
    # normalize_doc(doc.output)
    xc_doc=add_id(doc.output, xc_item)
    send_to_rabbitmq(xc_doc, xc_params)

fetch_url_raw_dag = fetch_url_raw()