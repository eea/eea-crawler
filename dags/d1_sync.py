from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

from tasks.helpers import dag_param_to_dict, load_variables, get_params
from lib.dagrun import trigger_dag
from lib import elastic
import json

default_args = {"owner": "airflow"}


default_dag_params = {
    "params": {"app": "datahub", "enable_prepare_docs": False}
}


@task
def create_raw_index(task_params):
    v = task_params.get("variables", {})
    elastic.create_raw_index(v)
    elastic.create_search_index(v)

    es = elastic.elastic_connection(v)
    elastic_conf = v.get("elastic")
    elastic.backup_indices(
        es, [elastic_conf["raw_index"], elastic_conf["searchui_target_index"]]
    )


@task
def trigger_all_crawlers(task_params, skip_docs):
    print(task_params)
    print(skip_docs)
    print(len(skip_docs))
    app = task_params["app"]
    Sites = task_params.get("Sites", [])
    if len(Sites) == 0:
        Sites = task_params.get("variables", {}).get("Sites", {}).keys()
    for site in Sites:
        crawl_config = {
            "params": {
                "site": site,
                "fast": task_params.get("fast", False),
                "ignore_delete_threshold": task_params.get(
                    "ignore_delete_threshold", False
                ),
                "app": app,
                "enable_prepare_docs": task_params.get(
                    "enable_prepare_docs", False
                ),
                "skip_docs": skip_docs,
            }
        }
        print(site)
        print(crawl_config)
        trigger_dag("d2_crawl_site", crawl_config, "default_pool")


@task
def test_errors(task_params):
    print(task_params)
    app = task_params["app"]
    v = task_params.get("variables", {})
    print(v)

    skip_cnt = v.get("skip_doc_cnt")
    errors_cnt = v.get("allowed_errors_for_doc")
    print("skip_cnt")
    print(skip_cnt)
    print("errors_cnt")
    print(errors_cnt)
    docs_with_errors = elastic.get_all_ids_with_error(v)
    print(docs_with_errors)
    print(len(docs_with_errors))

    error_var = f"errors_{app}"
    errors_from_vars = {}

    try:
        errors_from_vars = Variable.get(error_var, deserialize_json=True)
    except:
        Variable.set(error_var, json.dumps({}), description="failed documents")

    to_delete_from_vars = []
    for error_from_vars in errors_from_vars.keys():
        if not docs_with_errors.get(error_from_vars, False):
            to_delete_from_vars.append(error_from_vars)

    for to_delete in to_delete_from_vars:
        del errors_from_vars[to_delete]

    docs_to_skip = []

    for doc_with_errors in docs_with_errors.keys():
        if not errors_from_vars.get(doc_with_errors, False):
            errors_from_vars[doc_with_errors] = {"error_cnt": 1, "skip_cnt": 0}
        else:
            if errors_from_vars[doc_with_errors]["error_cnt"] >= errors_cnt:
                if errors_from_vars[doc_with_errors]["skip_cnt"] >= skip_cnt:
                    del errors_from_vars[doc_with_errors]
                else:
                    errors_from_vars[doc_with_errors]["skip_cnt"] += 1
                    docs_to_skip.append(doc_with_errors)
            else:
                errors_from_vars[doc_with_errors]["error_cnt"] += 1

    print(dir(Variable))
    Variable.update(error_var, json.dumps(errors_from_vars, indent=4))

    print(len(docs_to_skip))
    print(docs_to_skip)

    return docs_to_skip


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
    te = test_errors(xc_params)
    tac = trigger_all_crawlers(xc_params, te)
    cri >> te >> tac


crawl_dag = d1_sync()
