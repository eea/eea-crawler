from airflow.api.common.experimental.pool import create_pool, get_pool
from airflow.exceptions import PoolNotFound
from airflow.models import BaseOperator


class CreatePoolOperator(BaseOperator):
    # its pool blue, get it?
    ui_color = "#b8e9ee"
    template_fields = ["name", "slots"]

    # @apply_defaults
    def __init__(self, name, slots, description="", *args, **kwargs):
        super(CreatePoolOperator, self).__init__(*args, **kwargs)
        self.description = description
        self.slots = slots
        self.name = name

    def execute(self, context):
        try:
            pool = get_pool(name=self.name)
            if pool:
                self.log.info(f"Pool exists: {pool}")

        except PoolNotFound:
            # create the pool
            pool = create_pool(
                name=self.name, slots=self.slots, description=self.description
            )
            self.log.info(f"Created pool: {pool}")

        return self.name
