from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag
from tasks.helpers import get_app_identifier

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
def trigger_sync():
    app_id = get_app_identifier("global_search")
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
    trigger_sync()


sync_global_search_quick_dag = d0_sync_global_search_quick()
