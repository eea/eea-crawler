from airflow.decorators import task

from lib import elastic


@task
def create_raw_index():
    return simple_create_raw_index()
