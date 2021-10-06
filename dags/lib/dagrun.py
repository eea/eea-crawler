from datetime import timedelta
from tenacity import retry, wait_exponential, stop_after_attempt

from airflow.utils.session import provide_session
from airflow.utils import timezone
from airflow.models import DagRun
from airflow.utils.types import DagRunType
from airflow.api.common.experimental.trigger_dag import (
    trigger_dag as trigger_dag_original,
)


@retry(wait=wait_exponential(), stop=stop_after_attempt(10))
def trigger_dag_with_retry(trigger_dag_id, item, pool_name, session):
    delay = 2000
    execution_date = timezone.utcnow() + timedelta(microseconds=delay)
    run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)

    dag_run = trigger_dag_original(
        dag_id=trigger_dag_id,
        run_id=run_id,
        conf={"item": item},
        execution_date=execution_date,
        replace_microseconds=False,
    )

    tis = dag_run.get_task_instances()
    for ti in tis:
        ti.pool = pool_name
        session.add(ti)
        print("ti: %s", ti)


@provide_session
def trigger_dag(trigger_dag_id, item, pool_name, session):
    return trigger_dag_with_retry(trigger_dag_id, item, pool_name, session)
