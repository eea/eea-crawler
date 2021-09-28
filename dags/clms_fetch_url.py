from plone_restapi_harvester.harvester.harvester import harvest_document
import uuid
import json
from airflow.decorators import dag
from airflow.decorators import task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from eea.rabbitmq.client import RabbitMQConnector
from lib.debug import pretty_id
from plone_restapi_harvester.harvester.harvester import harvest_document
from tasks.debug import debug_value
from tasks.elastic import index_doc
from unidecode import unidecode
from urllib.parse import urlparse

import json
import re


# from haystack.preprocessor.preprocessor import PreProcessor
# from haystack.document_store.elasticsearch import ElasticsearchDocumentStore


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}

# Simply remove tags from text strings
# TODO: we need to preserve the links used behind a-tags some times,
# we ask question about what is the link for XYZ
def cleanhtml(raw_html):
    cleanr = re.compile("<.*?>")
    cleantext = re.sub(cleanr, "", raw_html)
    return cleantext


@task()
def clms_get_api_url(url):
    no_protocol_url = url.split("://")[-1]
    if "/api/" in no_protocol_url:
        print(no_protocol_url)
        return no_protocol_url
    url_parts = no_protocol_url.split("/")
    # url_parts.insert(1, "api")
    url_with_api = "/".join(url_parts)
    print(url_with_api)
    return url_with_api


@task
def my_harvest_document(doc):

    prepared_data_str = harvest_document(json.loads(doc))
    prepared_data = json.loads(prepared_data_str)
    prepared_data["id"] = prepared_data["absolute_url"]
    prepared_data["about"] = prepared_data["absolute_url"]
    return prepared_data


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def clms_fetch_url(
    item: str = "https://myrul",
):
    """
    this is a DAG definition it is not a proper python function
    so here we need to define all the required tasks and operators, and as a final
    step set the dependency-tree to let airflow know in which order to run the tasks
    """

    print("fetch_url", item)

    url_with_api = clms_get_api_url(item)

    doc = SimpleHttpOperator(
        task_id="get_doc",
        method="GET",
        endpoint=url_with_api,
        headers={"Accept": "application/json"},
    )

    prepared_data = my_harvest_document(doc.output)

    indexed_doc = index_doc(prepared_data)

    url_with_api >> doc >> prepared_data >> indexed_doc


clms_fetch_url_dag = clms_fetch_url()
