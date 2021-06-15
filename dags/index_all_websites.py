from airflow.decorators import dag  # , task
from airflow.models import Variable
from airflow.operators import trigger_dagrun
from airflow.utils.dates import days_ago

from lib.debug import hostname, pretty_id
from lib.pool import url_to_pool
from tasks.pool import CreatePoolOperator
from tasks.dagrun import BulkTriggerDagRunOperator
from tasks.debug import debug_value

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}

def get_indexed_websites():
    iw = "[]"
    try:
        iw = Variable.get("indexed_websites", deserialize_json=True)
    except:
        pass
    return iw

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["meta_workflow"],
)
def index_all_websites(
    websites: str = get_indexed_websites()
):
    """
    ### Triggers reindexing of all websites

    Reads the `INDEXED_WEBSITES` environment variable and triggers a reindexing
    DAG for all of them.
    """

    # configured_websites = Variable.get("indexed_websites", deserialize_json=True)

    debug_value(websites)

    BulkTriggerDagRunOperator(
        task_id="crawl_plonerestapi_websites",
        items=websites,
        trigger_dag_id="crawl_plonerestapi_website",
    )


index_all_websites_dag = index_all_websites()
