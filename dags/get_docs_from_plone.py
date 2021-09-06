import json

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from tasks.dagrun import BulkTriggerDagRunOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

from tasks.debug import debug_value

default_args = {
    "owner": "airflow",
}


@task()
def extract_docs_from_json(page):
    json_doc = json.loads(page)
    docs = json_doc['items']
    print(docs)
    return docs

@task()
def get_urls_from_docs(docs):
    urls = [doc["@id"] for doc in docs]
    return urls

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["test"],
)
def get_docs_from_plone(queryurl: str="www.eea.europa.eu/api/@search?portal_type=Highlight&sort_order=reverse&sort_on=Date&created.query=2021/6/1&created.range=min&b_size=500"):
    page = SimpleHttpOperator(
        task_id="get_docs_request",
        method="GET",
        endpoint=queryurl,
        headers={"Accept": "application/json"},
        )

    docs = extract_docs_from_json(page.output)
    debug_value(docs)
    urls = get_urls_from_docs(docs)
    debug_value(urls)
    
#    page = requests.get(SEARCHURL % (portal_type),  headers={'Accept': 'application/json'})
#    docs = page.json()['items']
#    print(len(docs))
#    return docs
   

    BulkTriggerDagRunOperator(
        task_id="fetch_urls",
        items=urls,
        trigger_dag_id="fetch_url",
    )

get_docs_from_plone_dag = get_docs_from_plone()