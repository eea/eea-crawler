from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from airflow import settings
import os

# session = settings.Session()


# from airflow.models import (
#     DAG,
#     DagModel,
#     DagRun,
#     Log,
#     SlaMiss,
#     TaskInstance,
#     Variable,
#     XCom,
# )


def clean_log(dag_id, logs_dir, older_than=2, prefix="dag_id="):
    print("CLEAN LOGS FOR")
    print(dag_id)
    dir_to_check = f'{logs_dir}{prefix}{dag_id}'
    print(dir_to_check)
#    print(os.listdir(dir_to_check))
    os.system(f"find {dir_to_check} -mtime +{older_than} -delete")

@task
def clean_logs(logs_dir, older_than):
    clean_log("d0_sync_global_search", logs_dir, older_than)
    clean_log("d0_sync_global_search_quick", logs_dir, older_than)
    clean_log("d0_sync_sdi", logs_dir, older_than)
    clean_log("d0_sync_sdi_demo", logs_dir, older_than)
    clean_log("d0_sync_wise_test", logs_dir, older_than)
    clean_log("d0_update_obligations", logs_dir, older_than)
    clean_log("d0_update_themetaxonomy", logs_dir, older_than)
    clean_log("d1_sync", logs_dir, older_than)
    clean_log("d2_crawl_site", logs_dir, older_than)
    clean_log("d3_crawl_fetch_for_id", logs_dir, older_than)
    clean_log("d5_prepare_doc_for_searchui", logs_dir, older_than)
    clean_log("scheduler", logs_dir, older_than, prefix="")



default_args = {"owner": "airflow"}


@dag(
    default_args=default_args,
    start_date=days_ago(1),
    description="maintenance",
    tags=["maintenance"],
    catchup=False,
    schedule_interval="0 2 * * *",
)
def d0_clean_daglogs(logs_dir="/opt/airflow/logs/", older_than=2):
    clean_logs(logs_dir, older_than)


clean_logs_dag = d0_clean_daglogs()
