import json
from airflow.decorators import task
from eea.rabbitmq.client import RabbitMQConnector


@task
def send_to_rabbitmq(doc, config):
    return simple_send_to_rabbitmq(doc, config)


def simple_send_to_rabbitmq(doc, config):
    rabbit_config = {
        "rabbit_host": config["host"],
        "rabbit_port": config["port"],
        "rabbit_username": config["username"],
        "rabbit_password": config["password"],
    }

    queue_name = config["queue"]

    rabbit = RabbitMQConnector(**rabbit_config)
    rabbit.open_connection()
    rabbit.declare_queue(queue_name)
    rabbit.send_message(queue_name, json.dumps(doc))
    rabbit.close_connection()
