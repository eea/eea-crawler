import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.http.operators.http import SimpleHttpOperator
from tasks.debug import debug_value

default_args = {
    "owner": "airflow",
}

@task
def dummy_get_docs():
    return [
        "Maritime transport plays and will continue to play an essential role in global and European trade and economy.",
        "The European Environment Agency provides sound, independent information on the environment for those involved in developing, adopting, implementing and evaluating environmental policy, and also the general public.",
        "Climate-friendly practices for sourcing raw materials hold significant potential to cut greenhouse gas emissions in Europe and globally."
    ]

@task
def build_data(docs):
    return json.dumps({
        "snippets":docs
    })
@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["semantic-search"],
)
def nlp_demo():
    """
    ### get info about an url
    """
    x_docs = dummy_get_docs()
    x_data = build_data(x_docs)
    debug_value(x_data)
    page = SimpleHttpOperator(
        http_conn_id='pure_http',
        task_id="nlp_request",
        endpoint="nlp-embedding/api/embedding/",
        data=x_data,
        method="POST",
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
    debug_value(page.output)

nlp_demo_dag = nlp_demo()