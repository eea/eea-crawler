from airflow.decorators import task


@task()
def show_dag_run_conf(conf):
    # start_url, maintainer_email="no-reply@plone.org"
    print("DAG RUN CONF", conf)


@task
def debug_value(val):
    print("type:", type(val))
    print("value:", val)
