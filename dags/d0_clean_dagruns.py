from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from airflow import settings

session = settings.Session()


from airflow.models import (
    DAG,
    DagModel,
    DagRun,
    Log,
    SlaMiss,
    TaskInstance,
    Variable,
    XCom,
)


def clean_dag(dag_id, size):
    airflow_db_model = DagRun

    while True:
        query = (
            session.query(airflow_db_model)
            .filter(DagRun.dag_id == dag_id)
            .limit(size)
        )
        print(dir(query))
        print("Query: " + str(query))
        entries_to_delete = query.all()
        if len(entries_to_delete) == 0:
            break

        print(dir(entries_to_delete[0]))
        entry_ids = [entry.run_id for entry in entries_to_delete]
        print(entry_ids)
        query2 = (
            session.query(airflow_db_model)
            .filter(DagRun.dag_id == dag_id)
            .filter(DagRun.run_id.in_(entry_ids))
        )
        print("Query2: " + str(query2))
        entries_to_delete2 = query2.all()
        print(len(entries_to_delete2))

        query2.delete(synchronize_session=False)
        session.commit()


@task
def clean_dagruns():
    print("HERE")
    # return
    print(dir(DagRun))
    clean_dag("d3_crawl_fetch_for_id", 100)
    clean_dag("d2_crawl_site", 10)
    clean_dag("d5_prepare_doc_for_searchui", 100)


default_args = {"owner": "airflow"}


@dag(
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    description="maintenance",
    tags=["maintenance"],
)
def d0_clean_dagruns():
    clean_dagruns()


update_clean_dagruns_dag = d0_clean_dagruns()
