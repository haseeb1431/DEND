from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key")
    copy_sql = """
        COPY {0}
        FROM '{1}'
        ACCESS_KEY_ID '{2}'
        SECRET_ACCESS_KEY '{3}'
        {4}
        {5}
        {6}
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="JSON",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = redshift_conn_id
        self.table=table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key        
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format


    def execute(self, context):
        self.log.info('StageToRedshiftOperator is implemented now')

        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        s3_hook = AwsHook(self.aws_credentials_id)
        credentials = s3_hook.get_credentials()

        self.log.info(f"Copying {self.table} from S3 to Redshift")
        s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key)
        if self.file_format == 'CSV':
            format = "CSV"
            delimiter = "DELIMITER ','"
            header = "IGNOREHEADER 1"
        else:
            format = "FORMAT AS JSON 'auto'"
            delimiter = ""
            header = ""

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            format,
            delimiter,
            header
        )
        
        pg_hook.run(formatted_sql)
        self.log.info(f"formatted query: {formatted_sql}")
        self.log.info(f"StageToRedshiftOperator completed: {self.table} ")

