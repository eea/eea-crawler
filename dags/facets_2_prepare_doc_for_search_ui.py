""" Given a source ES Doc id, cleanup and write the doc in an ES index

(via Logstash)
"""
import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from normalizers.registry import get_facets_normalizer
from tasks.helpers import simple_dag_param_to_dict, find_site_by_url
from tasks.rabbitmq import simple_send_to_rabbitmq
from tasks.elastic import get_doc_from_raw_idx
from airflow.models import Variable


default_args = {"owner": "airflow"}

default_dag_params = {
    "item": "https://water.europa.eu/marine/state-of-europe-seas/marine-sectors-catalogue-of-measures",
    "params": {},
    "site": "",
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    description="Normalize document metadata, prepare for facets",
)
def facets_2_prepare_doc_for_search_ui(item=default_dag_params):
    task_transform_doc(item)


def transform_doc(full_config):
    dag_params = simple_dag_param_to_dict(full_config, default_dag_params)

    site = find_site_by_url(dag_params["item"])

    es = Variable.get("elastic", deserialize_json=True)
    rabbitmq = Variable.get("rabbitmq", deserialize_json=True)
    rabbitmq["queue"] = rabbitmq["searchui_queue"]
    if dag_params["params"].get("raw_doc", None):
        doc = {
            "raw_value": dag_params["params"].get("raw_doc"),
            "web_text": dag_params["params"].get("web_text", ""),
            "pdf_text": dag_params["params"].get("pdf_text", ""),
        }
    else:
        doc = get_doc_from_raw_idx(dag_params["item"], es)

    sites = Variable.get("Sites", deserialize_json=True)

    site_config = Variable.get(sites[site], deserialize_json=True)
    normalizers_config = Variable.get(
        site_config["normalizers_variable"], deserialize_json=True
    )
    normalize = get_facets_normalizer(dag_params["item"])
    config = {
        "normalizers": normalizers_config,
        "nlp": site_config.get("nlp_preprocessing", None),
    }
    normalized_doc = normalize(doc, config)
    if normalized_doc:
        simple_send_to_rabbitmq(normalized_doc, rabbitmq)


@task
def task_transform_doc(full_config):
    transform_doc(full_config)


prepare_doc_for_search_ui_dag = facets_2_prepare_doc_for_search_ui()
