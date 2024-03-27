from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import requests
from lib import elastic
from lib.dagrun import trigger_dag
from tasks.helpers import dag_param_to_dict, load_variables, get_params
from tasks.pool import CreatePoolOperator
POOL_NAME = "mark_redirects_pool"
@task
def trigger_mark_redirects_bulk(task_params):
    v = task_params["variables"]
    docs = elastic.get_all_ids_from_searchui(v)
    cnt = 0
    bulks = []
    for doc_id, doc_value in docs.items():
        bulk_cnt = divmod(cnt, task_params["bulk_size"])
        if bulk_cnt[1] == 0:
            bulks.append([])
        bulks[bulk_cnt[0]].append({"doc_id":doc_id, "doc_value":doc_value})
        cnt+=1
        print(cnt)
    print(cnt)
    for bulk in bulks:
        bulk_config = {
            "item" : "",
            "params" : {
                "items": bulk,
                "app": task_params["app"]
            }
        }
        trigger_dag("d7_mark_redirects_bulk", bulk_config, POOL_NAME)


default_args = {"owner": "airflow"}
default_dag_params = {"item": "", "params": {"app": "global_search", "bulk_size":10000}}

@dag(
    default_args=default_args,
    start_date=days_ago(1),
    description="maintenance",
    tags=["maintenance"],
    catchup=False,
    schedule_interval=None,
)
def d6_mark_redirects(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)
    cpo = CreatePoolOperator(task_id="create_pool", name=POOL_NAME, slots=8)

    tmrb = trigger_mark_redirects_bulk(xc_params)
    cpo >> tmrb


mark_redirects_dag = d6_mark_redirects()
