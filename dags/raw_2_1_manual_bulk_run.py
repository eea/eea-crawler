import json
from urllib import robotparser
from urllib.parse import urlparse

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from tasks.dagrun import BulkTriggerDagRunOperator

from tasks.debug import debug_value
from tasks.helpers import (
    dag_param_to_dict,
    build_items_list,
    get_item,
)


default_args = {"owner": "airflow"}


default_dag_params = {
    "items": [],
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["trigger raw_3_fetch_url_raw for each document"],
    description="""Query site for a content type, trigger
    raw_3_fetch_url_raw for each document""",
)
def raw_2_1_manual_bulk_run(
    items=[
        "https://biodiversity.europa.eu/countries/estonia/maes/d1-1-final-methodological-report_july_2021.pdf"
    ],
):
    debug_value(items)

    xc_items = build_items_list(items, {})
    debug_value(xc_items)

    bt = BulkTriggerDagRunOperator(
        task_id="fetch_url_raw",
        items=xc_items,
        trigger_dag_id="raw_3_fetch_url_raw",
    )


manual_bulk_raw_dag = raw_2_1_manual_bulk_run()
