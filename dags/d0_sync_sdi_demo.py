from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag

START_DATE = days_ago(1)

app_config = Variable.get("app_datahub_demo", deserialize_json=True)

SCHEDULE_INTERVAL = app_config.get("schedule_interval", "@daily")
default_args = {"owner": "airflow"}


TASK_PARAMS = {"params": {"app": "datahub_demo", "enable_prepare_docs": True}}


@task
def trigger_sync(ignore_delete_threshold):
    print(ignore_delete_threshold)
    TASK_PARAMS["params"]["ignore_delete_threshold"] = ignore_delete_threshold
    print(TASK_PARAMS)
    trigger_dag("d1_sync", TASK_PARAMS, "default_pool")


@dag(
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=SCHEDULE_INTERVAL,
    description="scheduled sdi sync",
    tags=["crawl"],
)
def d0_sync_sdi_demo(ignore_delete_threshold=False):
    trigger_sync(ignore_delete_threshold)


sync_sdi_demo_dag = d0_sync_sdi_demo()
