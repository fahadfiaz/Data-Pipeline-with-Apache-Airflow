from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 tables=[], 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks
        self.tables = tables

    def execute(self, context):
        
        redshift = PostgresHook(self.redshift_conn_id)
        for i, dq_check in enumerate(self.dq_checks):
            records = redshift.get_records(dq_check['test_sql'])
            if not dq_check['expected_results'] == records[0][0]:
                self.log.info(f"Data quality check failed on {self.tables[i]} table")
                raise ValueError(f"Data quality check failed on {self.tables[i]} table")
            self.log.info(f"Data quality on table {self.tables[i]} check passed")