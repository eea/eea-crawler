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
app_config = Variable.get("app_global_search", deserialize_json=True)
SCHEDULE_INTERVAL = app_config.get("schedule_interval", None)
default_args = {"owner": "airflow"}
default_dag_params = {"item": "", "params": {"app": "global_search", "attr":"publisher"}}

def update_var(attr, key, site_id, doc_id):
    stored_attrs = Variable.get(f"_attr_find_{attr}", deserialize_json=True)
    stored_attrs_site = stored_attrs.get(site_id, None)
    if stored_attrs_site is None:
        stored_attrs[site_id] = {}
    
    if stored_attrs[site_id].get(key, None) is None:
        stored_attrs[site_id][key] = doc_id
        Variable.update(f"_attr_find_{attr}", json.dumps(stored_attrs, indent=4))

def doc_handler(v, doc_id, site_id, doc_handler):
    raw_doc = normalizer.get_raw_doc_by_id(v, doc_id)
    for key in raw_doc['raw_value'].keys():
        if v.get('attr') in key:
            doc_handler(v.get('attr'), key, site_id, raw_doc['raw_value']["@id"])

@task
def trigger_find(task_params):
    attr = task_params.get("attr")
    try:
        Variable.get(f"_attr_find_{attr}", deserialize_json=True)
    except:
        Variable.set(f"_attr_find_{attr}", json.dumps({}), description="failed documents")
    task_params['variables']["attr"] = attr
    normalizer.parse_all_documents(
        task_params["variables"], doc_handler, update_var
    )

    app_id = get_app_identifier("global_search")



@dag(
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    description="scheduled global search sync",
    tags=["crawl"],
)
def d0_find_attr(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)

    trigger_find(xc_params)


find_attr_dag = d0_find_attr()
