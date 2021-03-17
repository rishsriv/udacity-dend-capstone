from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        table="",
        sql_source="",
        operation_type="",
        *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_source = sql_source
        self.operation_type = operation_type

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.operation_type == "truncate" or self.operation_type == "truncate_then_append":
            self.log.info(f"Truncating {self.table} table")
            redshift.run(f"DELETE FROM self.table")
        
        if self.operation_type == "append" or self.operation_type == "truncate_then_append":
            sql_to_run = f"INSERT INTO {self.table} {self.sql_source}"
            self.log.info(f"Executing {sql_to_run} ...")
            redshift.run(sql_to_run)