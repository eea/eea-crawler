from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from tasks.helpers import dag_param_to_dict, load_variables, get_params
from lib.dagrun import trigger_dag
from lib import elastic

default_args = {"owner": "airflow"}


default_dag_params = {
    "params": {
        "Sites": [
            "ias",
            "sdi",
            "industry",
            "bise",
            "climate",
            "eionet",
            "energy",
            "fise",
            "wise_freshwater",
            "wise_marine",
        ]
    }
}


@task
def create_raw_index(task_params):
    v = task_params.get("variables", {})
    elastic.create_raw_index(v)
    es = elastic.elastic_connection(v)
    elastic_conf = v.get("elastic")
    elastic.backup_indices(es, [elastic_conf["raw_index"]])


@task
def trigger_all_crawlers(task_params):
    Sites = task_params.get("Sites", [])
    if len(Sites) == 0:
        Sites = task_params.get("variables", {}).get("Sites", {}).keys()
    for site in Sites:
        crawl_config = {"params": {"site": site, "fast": False}}
        print(site)
        print(crawl_config)
        trigger_dag("d2_crawl_site", crawl_config, "default_pool")


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    description="Entrypoint for sync",
    tags=["crawl"],
)
def d1_sync(item=default_dag_params):
    xc_dag_params = dag_param_to_dict(item, default_dag_params)
    xc_params = get_params(xc_dag_params)
    xc_params = load_variables(xc_params)
    cri = create_raw_index(xc_params)
    tac = trigger_all_crawlers(xc_params)
    cri >> tac


crawl_dag = d1_sync()
