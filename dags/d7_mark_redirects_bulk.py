from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow import DAG

import requests
from lib import elastic
from tasks.helpers import dag_param_to_dict, load_variables, get_params
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from lib import rabbitmq

POOL_NAME = "mark_redirects_pool"

def send_to_rabbitmq(v, doc):
    print(v)
    print("send_to_rabbitmq:")
    print(doc)
    index_name = v.get("elastic", {}).get("searchui_target_index", None)
    print(index_name)
    if index_name is not None:
        doc["index_name"] = index_name
        rabbitmq_config = v.get("rabbitmq")
        print(rabbitmq_config)
        print(doc)
        rabbitmq.send_to_rabbitmq(doc, rabbitmq_config)

def url_redirects(url):
    try:
        r = requests.head(url)
        if r.is_redirect:
            return True
        else:
            return False
    except:
        return False

def update_redirect_in_es(v, url, redirect):
    doc = {"update_only": True, "id":url}
    if redirect:
        doc['exclude_from_globalsearch'] = 'redirected'
        print("REDIRECTED YES")
    else:
        doc['exclude_from_globalsearch'] = None
        print("REDIRECTED NO")
    #send_to_rabbitmq(v, doc)


def _mark_redirects(task_params):
    v = task_params["variables"]
    cnt = 0
    for item in task_params["items"]:
        doc_id = item["doc_id"]
        print(item)
        print(cnt)
        doc_value = item["doc_value"]
        cnt += 1
        redirected = url_redirects(doc_id)
        #
        #redirected = True
        #
        already_redirected = False
        skip_redirect = False
        if doc_value.get("exclude_from_globalsearch", None) != None:
            if doc_value.get("exclude_from_globalsearch", None) == 'redirected':
                already_redirected = True
            else:
                print("Skip redirect flag")
                skip_redirect = True
        if not skip_redirect:
            if redirected != already_redirected:
                update_redirect_in_es(v, doc_id, redirected)

class MROperator(BaseOperator):
    template_fields = ["task_params"]

    def __init__(self, task_params, *args, **kwargs):
        super(MROperator, self).__init__(*args, **kwargs)
        self.task_params = task_params

    def execute(self, context):
        _mark_redirects(self.task_params)

default_args = {"owner": "airflow"}
default_dag_params = {"item": "", "params": {"app": "global_search", "links":[]}}

@dag(
    default_args=default_args,
    start_date=days_ago(1),
    description="maintenance",
    tags=["maintenance"],
    catchup=False,
    schedule_interval=None,
)
def d7_mark_redirects_bulk(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)
    MROperator(task_id="mark_redirects", task_params = xc_params, pool=POOL_NAME)

mark_redirects_bulk_dag = d7_mark_redirects_bulk()
