import json
from airflow.decorators import task
from eea.rabbitmq.client import RabbitMQConnector


@task
def send_to_rabbitmq(doc, params):
    return simple_send_to_rabbitmq(doc, params)


def simple_send_to_rabbitmq(doc, params):
    rabbit_config = {
        "rabbit_host": params['rabbitmq']['host'],
        "rabbit_port": params['rabbitmq']['port'],
        "rabbit_username": params['rabbitmq']['username'],
        "rabbit_password": params['rabbitmq']['password'],
    }
    params['rabbitmq']
    queue_name = params['rabbitmq']['queue']

    rabbit = RabbitMQConnector(**rabbit_config)
    rabbit.open_connection()
    rabbit.declare_queue(queue_name)
    rabbit.send_message(queue_name, json.dumps(doc))
    rabbit.close_connection()
