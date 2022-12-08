from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 file_type="json",
                 delimiter=",",
                 ignore_header=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.file_type = file_type
        self.delimiter = delimiter
        self.ignore_header = ignore_header

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Remove {self.table} table data from Redshift")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        file_dependent_parameter = ""
        if self.file_type == "json":
            file_dependent_parameter = "JSON '{}'".format(self.json_path)
        elif self.file_type == "csv":
            file_dependent_parameter = "IGNOREHEADER '{}' DELIMITER '{}'".format(self.ignore_header, self.delimiter)

            
        sql_query = """
            COPY "dev"."public".{}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            {}
            region 'us-west-2';
            """
        
        sql_cmd = sql_query.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            file_dependent_parameter
        )
        
        redshift.run(sql_cmd)
        self.log.info(f"{self.table} table data copied from S3 to Redshift")