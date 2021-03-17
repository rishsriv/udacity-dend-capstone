from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="redshift",
        dq_checks=None,
        *args, **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for item in self.dq_checks:
            records = redshift_hook.get_records(item['check_sql'])
            if item['expected_result_type'] == "has_values":
                if len(records) < 1 or len(records[0]) < 1:
                    raise ValueError(f"Data quality check failed. {item['check_sql']} returned null")
                num_records = records[0][0]
                if num_records < 1:
                    raise ValueError(f"Data quality check failed. {item['check_sql']} has 0 rows")
                self.log.info(f"Data quality check passed for {item['check_sql']}. It has {records[0][0]} rows")