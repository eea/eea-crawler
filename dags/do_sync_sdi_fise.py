from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag
from tasks.helpers import get_app_identifier, get_next_execution_date_for_dag, load_variables, dag_param_to_dict, get_params
from lib import elastic, status

START_DATE = days_ago(1)

app_config = Variable.get("app_fise_sdi", deserialize_json=True)

SCHEDULE_INTERVAL = app_config.get("schedule_interval", "@yearly")
default_args = {"owner": "airflow"}


TASK_PARAMS = {"params": {"app": "fise_sdi", "enable_prepare_docs": True}}


@task
def trigger_sync(ignore_delete_threshold, task_params):
    print("d0_sync_sdi_fise trigger_sync 0")
    print(ignore_delete_threshold)
    TASK_PARAMS["params"]["ignore_delete_threshold"] = ignore_delete_threshold
    print('d0_sync_sdi_fise TASK_PARAMS')
    print(TASK_PARAMS)
    print("d0_sync_sdi_fise trigger_sync 1")
    trigger_dag("d1_sync", TASK_PARAMS, "default_pool")


@dag(
    default_args=default_args,
    start_date=START_DATE,
    catchup=False,
    schedule_interval=SCHEDULE_INTERVAL,
    description="scheduled sdi sync for fise",
    tags=["crawl"],
)
def d0_sync_sdi_fise(ignore_delete_threshold=False):
    print("do_sync_sdi 0")
    xc_dag_params = dag_param_to_dict(TASK_PARAMS)
    print("do_sync_sdi 1")
    xc_params = get_params(xc_dag_params)
    print("do_sync_sdi 2")
    xc_params = load_variables(xc_params)
    print("do_sync_sdi 3 {}".format(xc_params))
    trigger_sync(ignore_delete_threshold, xc_params)
    print("do_sync_sdi 4")


sync_sdi_dag = d0_sync_sdi_fise()
