from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from app import sdi_crawl

from lib.dagrun import trigger_dag

import logging

logger = logging.getLogger(__file__)

from tasks.helpers import (
    dag_param_to_dict,
    load_variables,
    get_params,
)

#from tasks.elastic import create_raw_index
#from tasks.pool import CreatePoolOperator
#from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.debug import debug_value
from tasks.pool import CreatePoolOperator

default_args = {"owner": "airflow"}


default_dag_params = {"params": {"site": "sdi", "fast": True}}

def doc_handler(params, metadataIdentifier):
    print("PARAMS")
    task_params = {"params": params, "item": metadataIdentifier}
    print(metadataIdentifier)
    trigger_dag('sdi_2_fetch_for_id', task_params, 'sdi')

@task
def create_raw_index(task_params):
    sdi_crawl.create_raw_index(task_params['variables'])

@task
def parse_all_documents(task_params):
    handler = sdi_crawl.crawl_doc
    if not task_params["fast"]:
        handler = doc_handler
    sdi_crawl.parse_all_documents(task_params['variables'], handler)

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    description="Entrypoint for crawling sdi",
    tags=["sdi"],
)
def crawl_1_site(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)
    cri = create_raw_index(xc_params)
    pd = parse_all_documents(xc_params)
    cpo = CreatePoolOperator(task_id="create_pool", name="sdi", slots=4)

    cri >> cpo >> pd


crawl_dag = crawl_1_site()

