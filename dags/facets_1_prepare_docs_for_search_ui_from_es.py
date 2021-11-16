""" Get all doc ids from an ES index and triggers preprocessing for each Doc ID
"""

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.pool import CreatePoolOperator

from tasks.debug import debug_value
from tasks.helpers import (
    dag_param_to_dict,
    get_item,
    set_attr,
    get_variable,
)
from lib.pool import url_to_pool


from tasks.elastic import create_index, handle_all_ids
from facets_2_prepare_doc_for_search_ui import transform_doc
from airflow.models import Variable

# import json
# from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {"owner": "airflow"}

default_dag_params = {
    "item": "http://www.eea.europa.eu/api/@search?portal_type=Highlight&sort_order=reverse&sort_on=Date&created.query=2021/6/1&created.range=min&b_size=500",
    "params": {
        "fast": False,
        "portal_type": "",
        "site": "",
    },
}


@task
def get_es_config():
    elastic = Variable.get("elastic", deserialize_json=True)
    elastic["target_index"] = elastic["searchui_target_index"]
    return elastic


@task
def get_rabbitmq_config():
    rabbitmq = Variable.get("rabbitmq", deserialize_json=True)
    rabbitmq["queue"] = rabbitmq["searchui_queue"]
    return rabbitmq


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["trigger facets_2_prepare_doc_for_search_ui for each document"],
    description="Query the raw index (optional only for a type), trigger facets_2_prepare_doc_for_search_ui for each document",
)
def facets_1_prepare_docs_for_search_ui_from_es(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)

    xc_item = get_item(xc_dag_params)
    xc_es = get_es_config()
    xc_es_mapping = get_variable("elastic_mapping")
    xc_es_settings = get_variable("elastic_settings")

    xc_es = set_attr(xc_es, "mapping", xc_es_mapping)
    xc_es = set_attr(xc_es, "settings", xc_es_settings)

    create_index(xc_es)

    xc_pool_name = url_to_pool(xc_item, prefix="prepare_doc_for_search_ui")
    cpo = CreatePoolOperator(
        task_id="create_pool", name=xc_pool_name, slots=16
    )

    handle_all_ids(
        xc_es,
        xc_dag_params,
        xc_pool_name,
        "facets_2_prepare_doc_for_search_ui",
        handler=transform_doc,
    )


prepare_docs_for_search_ui_from_es_dag = (
    facets_1_prepare_docs_for_search_ui_from_es()
)
