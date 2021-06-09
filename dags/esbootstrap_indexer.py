from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from tasks.debug import debug_value
default_args = {
    "owner": "airflow",
}
@task 
def build_command(app_name):
    cmd = 'APP_CONFIG_DIRNAME=%s  elastic_host=elastic /code/app.js create_index' %app_name
    print(cmd)
    return cmd

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["esbootstrap"],
)
def esbootstrap_nodejs_indexer(app_name: str = 'pam'):
    debug_value(app_name)
    cmd = build_command(app_name)
    print(cmd)
    esbootstrap_indexer_task = BashOperator(
        task_id="bash_task",
        bash_command=cmd,
    )

esbootstrap_nodejs_indexer_dag = esbootstrap_nodejs_indexer()