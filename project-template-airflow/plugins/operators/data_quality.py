from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info(f'DataQualityOperator {self.table} is now running')
        pg_hook = PostgresHook(self.conn_id)
        
        for tbl in self.tables:        
            records = pg_hook.get_records(f"SELECT COUNT(*) FROM {tbl}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {tbl} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {tbl} contained 0 rows")
            self.log.info(f"Data quality on table {tbl} is now completed. Records {num_records} ")
        
        self.log.info(f"Data quality is now completed.")
