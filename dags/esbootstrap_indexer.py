"""
Uses `eea.searchserver` scripts to index content in an ES index
"""
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from tasks.debug import debug_value

default_args = {
    "owner": "airflow",
}


# TODO: run directly in the bash operator, using Jinja template
@task
def build_command(app_name):
    cmd = (
        "APP_CONFIG_DIRNAME=%s elastic_host=elastic /code/app.js create_index"
        % app_name
    )
    print(cmd)
    return cmd


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["esbootstrap"],
)
def esbootstrap_script(
    app_name: str = "global-search", command: str = "create_index"
):
    debug_value(app_name)
    # cmd = build_command(app_name)
    # print(cmd)

    cmd = (
        'APP_CONFIG_DIRNAME="{{params.app_name}}" '
        "elastic_host=elastic /code/app.js {{params.command}}"
    )

    esbootstrap_script_task = BashOperator(
        task_id="esbootstrap_script_task",
        bash_command=cmd,
        params={app_name: app_name, command: command},
    )


esbootstrap_script_dag = esbootstrap_script()
