from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table = "",
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate
        

    def execute(self, context):       
        redshift = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Creating {self.table} table")
        
        sql_cmd = ""
        
        if self.truncate:
            
            sql_query = """
            TRUNCATE TABLE {}; 
            
            INSERT INTO {}
            {};
            """
        
            sql_cmd = sql_query.format(
                self.table,
                self.table,
                self.sql_query
            )
        
        else:
            sql_query = """
                INSERT INTO {}
                {};
                """

            sql_cmd = sql_query.format(
                self.table,
                self.sql_query
            )
        
        redshift.run(sql_cmd)
        self.log.info(f"Success: {self.table} table created")