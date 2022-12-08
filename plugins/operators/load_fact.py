from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
    

    def execute(self, context):
 
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating songplays table")
        
        
        sql_query = """
            INSERT INTO {}
            {};
            """
        
        sql_cmd = sql_query.format(
            self.table,
            self.sql_query
        )
        
        redshift.run(sql_cmd)
        self.log.info("Success: songplays table created")