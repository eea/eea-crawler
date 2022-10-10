from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag

START_DATE = days_ago(1)

app_config = Variable.get("app_datahub", deserialize_json=True)

SCHEDULE_INTERVAL = app_config.get("schedule_interval", "@daily")
default_args = {"owner": "airflow"}


TASK_PARAMS = {"params": {"app": "datahub", "enable_prepare_docs": True}}


@task
def trigger_sync():
    pass
    trigger_dag("d1_sync", TASK_PARAMS, "default_pool")


@dag(
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=SCHEDULE_INTERVAL,
    description="scheduled sdi sync",
    tags=["crawl"],
)
def d0_sync_sdi():
    trigger_sync()


sync_sdi_dag = d0_sync_sdi()