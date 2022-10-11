from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


from lib.dagrun import trigger_dag
from normalizers import normalizer

import logging

logger = logging.getLogger(__file__)
POOL_NAME = "prepare_for_searchui"
POOL_SLOTS = 16

from lib import rabbitmq

from tasks.helpers import dag_param_to_dict, load_variables, get_params

from tasks.pool import CreatePoolOperator
from tasks.debug import debug_value

default_args = {"owner": "airflow"}


default_dag_params = {"item": "", "params": {"app": "datahub"}}


def send_to_rabbitmq(v, doc):
    print("send_to_rabbitmq:")
    print(doc)

    index_name = v.get("elastic", {}).get("searchui_target_index", None)
    if index_name is not None:
        doc["index_name"] = index_name
        rabbitmq_config = v.get("rabbitmq")
        rabbitmq.send_to_rabbitmq(doc, rabbitmq_config)


def doc_handler(v, doc_id, site_id, doc_handler):
    task_params = {"item": doc_id, "params": {"site": site_id, "variables": v}}
    print(task_params)
    trigger_dag("d5_prepare_doc_for_searchui", task_params, POOL_NAME)


@task
def parse_all_documents(task_params):
    handler = normalizer.preprocess_doc
    if not task_params.get("fast", None):
        handler = doc_handler

    normalizer.parse_all_documents(
        task_params["variables"], handler, send_to_rabbitmq
    )


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    description="Entrypoint for preprocessing",
    tags=["preprocess"],
)
def d4_prepare_docs_for_searchui(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)
    cpo = CreatePoolOperator(
        task_id="create_pool", name=POOL_NAME, slots=POOL_SLOTS
    )
    pd = parse_all_documents(xc_params)

    cpo >> pd


prepare_docs_for_searchui_dag = d4_prepare_docs_for_searchui()
