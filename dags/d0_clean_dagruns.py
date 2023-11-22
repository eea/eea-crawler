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


def clean_dag(dag_id, size, older_than=2, dag_state='success'):
    airflow_db_model = DagRun
    import datetime, pytz
    tod = datetime.datetime.now(pytz.utc)
    d = datetime.timedelta(days = older_than)
    older_than_date = tod - d

    while True:
        query = (
            session.query(airflow_db_model)
            .filter(DagRun.dag_id == dag_id)
            .filter(DagRun.state == dag_state)
            .filter(DagRun.start_date < older_than_date)
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
def clean_dagruns(older_than, dag_states):
    for dag_state in dag_states:
        clean_dag("d0_sync_global_search", 100, older_than, dag_state)
        clean_dag("d0_sync_global_search_quick", 100, older_than, dag_state)
        clean_dag("d0_sync_sdi", 100, older_than, dag_state)
        clean_dag("d0_update_obligations", 100, older_than, dag_state)
        clean_dag("d0_update_themetaxonomy", 100, older_than, dag_state)
        clean_dag("d1_sync", 100, older_than, dag_state)
        clean_dag("d2_crawl_site", 100, older_than, dag_state)
        clean_dag("d3_crawl_fetch_for_id", 100, older_than, dag_state)
        clean_dag("d5_prepare_doc_for_searchui", 100, older_than, dag_state)


default_args = {"owner": "airflow"}


@dag(
    default_args=default_args,
    start_date=days_ago(1),
    description="maintenance",
    tags=["maintenance"],
    catchup=False,
    schedule_interval="0 2 * * *",
)

def d0_clean_dagruns(older_than=2, dag_states=['success', 'failed']):
    clean_dagruns(older_than, dag_states)


update_clean_dagruns_dag = d0_clean_dagruns()
