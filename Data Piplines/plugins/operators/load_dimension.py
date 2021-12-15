from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_statement="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_statement = insert_statement

    def execute(self, context):
        """
        1. connect postgre
        2. insert data into dim table
        """
        # connect redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clearing data
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))

        # statement of inserting data
        redshift.run(self.insert_statement)
        self.log.info(f"Data Insert Passed on table : {self.table}!!!") 
