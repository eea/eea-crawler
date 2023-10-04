from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag
from tasks.helpers import get_app_identifier, get_next_execution_date_for_dag, load_variables, dag_param_to_dict, get_params
from lib import elastic, status

START_DATE = days_ago(1)
app_config = Variable.get("app_global_search", deserialize_json=True)
SCHEDULE_INTERVAL = app_config.get("schedule_interval_quick", None)
default_args = {"owner": "airflow"}

TASK_PARAMS = {
    "params": {
        "app": "global_search",
        "enable_prepare_docs": True,
        "quick": True,
        "ignore_delete_threshold": False,
    }
}


@task
def trigger_sync(task_params):
    next_execution_date = get_next_execution_date_for_dag("d0_sync_global_search_quick")
    TASK_PARAMS["params"]["next_execution_date"] = next_execution_date
    v = task_params.get("variables", {})
    elastic.create_status_index(v)
    v['next_execution_date'] = next_execution_date
    try:
        app_id = get_app_identifier("global_search")
        status.add_site_status(v, task_name='scheduled', status = 'Started')
    except Exception as e:
        status.add_site_status(v, task_name='scheduled', msg = str(e), status = 'Failed')
        raise (Exception)

    TASK_PARAMS["params"]["app_identifier"] = app_id
    trigger_dag("d1_sync", TASK_PARAMS, "default_pool")


@dag(
    default_args=default_args,
    start_date=START_DATE,
    catchup=False,
    schedule_interval=SCHEDULE_INTERVAL,
    description="scheduled global search sync fast",
    tags=["crawl"],
)
def d0_sync_global_search_quick():
    xc_dag_params = dag_param_to_dict(TASK_PARAMS)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)
    trigger_sync(xc_params)


sync_global_search_quick_dag = d0_sync_global_search_quick()
