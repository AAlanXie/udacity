from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables


    def execute(self, context):
        """
        1. connect postgre
        2. Determine the number of rows in each table
        """
        # connect redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clearing data
        self.log.info("Clearing data from destination Redshift table")

        for table in self.tables:
            # check data quality on every table   
            self.log.info(f"Starting data quality validation on table : {table}")
            records = redshift.get_records(f"select count(1) from {table};")

            # Determine the number of rows in each table
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data Quality validation failed for table : {table}.")
                raise ValueError(f"Data Quality validation failed for table : {table}")
            self.log.info(f"Data Quality Validation Passed on table : {table}!!!") 
