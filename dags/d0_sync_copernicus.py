from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag
from tasks.helpers import get_app_identifier

START_DATE = days_ago(1)
app_config = Variable.get("app_copernicus", deserialize_json=True)
SCHEDULE_INTERVAL = app_config.get("schedule_interval", "@daily")
default_args = {"owner": "airflow"}

TASK_PARAMS = {"params": {"app": "copernicus", "enable_prepare_docs": True}}


@task
def trigger_sync(ignore_delete_threshold):
    app_id = get_app_identifier("copernicus_test")
    TASK_PARAMS["params"]["ignore_delete_threshold"] = ignore_delete_threshold
    TASK_PARAMS["params"]["app_identifier"] = app_id
    TASK_PARAMS["params"]["skip_status"] = True
    trigger_dag("d1_sync", TASK_PARAMS, "default_pool")


@dag(
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    description="scheduled copernicus sync",
    tags=["crawl"],
)
def d0_sync_copernicus(ignore_delete_threshold=False):
    trigger_sync(ignore_delete_threshold)


sync_copernicus_dag = d0_sync_copernicus()
