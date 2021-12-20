from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, 
                               LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries
from airflow.operators import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


# dag configuration
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2021, 12, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup': False,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs = 1
        )

# operators
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table_operate = PostgresOperator(
    task_id="Create_tables",
    dag=dag,
    postgres_conn_id='redshift',
    sql=open('/home/workspace/airflow/create_tables.sql', 'r').read()
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_path="s3://udacity-dend/log_data",
    file_format='JSON',
    log_json_file='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_path="s3://udacity-dend/song_data",
    file_format='JSON'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    insert_statement=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    insert_statement=SqlQueries.user_table_insert,
    mode='append_only'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    insert_statement=SqlQueries.song_table_insert,
    mode='append_only'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    insert_statement=SqlQueries.artist_table_insert,
    mode='append_only'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    insert_statement=SqlQueries.time_table_insert,
    mode='append_only'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# dependencies
# start - > stage operators
start_operator >> create_table_operate

create_table_operate >> stage_events_to_redshift
create_table_operate >> stage_songs_to_redshift

# stage operators - > load fact table
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# load fact table - > load dim table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# load dim table >> check quality
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# check quality - > stop
run_quality_checks >> end_operator