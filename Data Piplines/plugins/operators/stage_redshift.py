from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    :desc plugins with the function of loading data into redshift from s3
    """
    ui_color = '#358140'
    copy_sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 file_format="",
                 log_json_file="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.aws_credentials_id = aws_credentials_id
        self.file_format = file_format
        self.log_json_file = log_json_file

    def execute(self, context):
        """
        1. connect aws
        2. connect redshift
        3. fill the copy_sql_template with personal parameters
        4. copy the data from s3 to redshift
        """
        # connect to AWS
        aws_hook = AwsHook(self.aws_credentials_id)
        # get the parameters of credentials
        credentials = aws_hook.get_credentials()
        # connect to postgre
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clearing data
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift.run("DELETE FROM {}".format(self.table))

        # start to copy data from s3 to redshift
        self.log.info(f"Copying data from S3 to Redshift table {self.table}")
        if self.log_json_file != "":
            formatted_sql = StageToRedshiftOperator.copy_sql_template.format(
                self.table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.log_json_file
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql_template.format(
                self.table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
                'auto'
            )
        redshift.run(formatted_sql)
        self.log.info(f"{self.table} staged successfully!!!")
