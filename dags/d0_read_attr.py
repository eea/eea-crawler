from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from lib.dagrun import trigger_dag
from tasks.helpers import get_app_identifier
from normalizers import normalizer
from tasks.helpers import dag_param_to_dict, load_variables, get_params
from airflow.models import Variable
import json
START_DATE = days_ago(1)
SCHEDULE_INTERVAL = None
default_args = {"owner": "airflow"}


@task
def trigger_read():
    attr = Variable.get(f"_attr_find_publisher", deserialize_json=True)
    print(json.dumps(attr))



@dag(
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    description="scheduled global search sync",
    tags=["crawl"],
)
def d0_read_attr():
    trigger_read()


read_attr_dag = d0_read_attr()
