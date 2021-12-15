from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_statement = insert_statement

    def execute(self, context):
        # connect redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clearing data
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))

        # statement of inserting data
        self.log.info(f'Start to insert data into table {self.table}')
        redshift.run(self.insert_statement)
        self.log.info(f"Data Insert Passed on table : {self.table}!!!") 
