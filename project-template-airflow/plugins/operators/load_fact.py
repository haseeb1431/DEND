from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="redshift",
                 table="",
                 sql_stmt="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt

    def execute(self, context):
        self.log.info(f'LoadFactOperator {self.table} is now running')

        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_stmt})"
        pg_hook.run(formatted_sql)
        self.log.info(f"formatted query: {formatted_sql}")
        self.log.info(f'LoadFactOperator {self.table} is now completed')
