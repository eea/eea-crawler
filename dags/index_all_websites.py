from airflow.decorators import dag  # , task
from airflow.models import Variable
from airflow.operators import trigger_dagrun
from airflow.utils.dates import days_ago

from lib.debug import hostname, pretty_id
from lib.pool import url_to_pool
from tasks.pool import CreatePoolOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["meta_workflow"],
)
def index_all_websites():
    """
    ### Triggers reindexing of all websites

    Reads the `INDEXED_WEBSITES` environment variable and triggers a reindexing
    DAG for all of them.
    """

    # configured_websites = Variable.get("indexed_websites", deserialize_json=True)

    configured_websites = ["https://eea.europa.eu"]
    #    helpers.debug_value(configured_websites)

    for site_url in configured_websites:
        task_id = "trigger_crawl_dag_" + pretty_id(site_url)
        t = trigger_dagrun.TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id="crawl_plonerestapi_website",
            conf={
                "website_url": site_url,
                # TODO: read also maintainer from configuration
                "maintainer_email": "tibi@example.com",
                "allocated_api_pool": "api_{}".format(hostname(site_url))[:40],
            },
        )
        cpo = CreatePoolOperator(
            task_id="create_pool_" + pretty_id(site_url),
            name=url_to_pool(site_url),
            slots=2,
        )

        t >> cpo


index_all_websites_dag = index_all_websites()
