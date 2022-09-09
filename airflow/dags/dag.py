from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

""" 
This Apache Airflow DAG provides a pipeline to:
 - Copy data from AWS S3 to AWS Redshift staging tables:
    - Stage_events;
    - Stage_songs.
 - Load data from staging tables to dimensions tables:
    - Load_time_dim_table;
    - Load_user_dim_table;
    - Load_artist_dim_table;
    - Load_song_dim_table.
 - Load data from staging tables to fact table:
    - Load_songplays_fact_table.
 - Verify the data loaded to fact and dimension tables:
    - Run_data_quality_checks.
* As a default:
    * Runs daily;
    * Starts from 2018-11-02 for previous day (from 2018-11-01);
    * In case of failure - DAG retries 3 times, after 5 min delay;
    * Max active runs - 1.

Datapipeline scheme:

B{{Begin_execution}} -->E(Stage_vents)
B --> S(Stage_songs)
    E --> U(Load_user_dim_table)
    E --> T(Load_time_dim_table)
	S --> A(Load_artist_dim_table)
		A --> Sng(Load_song_dim_table)
			U --> F(Load_songplays_fact_table)
			T --> F
			Sng --> F
				F --> C(Run_data_quality_checks)
					C --> End{{End_execution}}

"""

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 2),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('dag',
          max_active_runs=1,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift =  StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table="public.staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_paths="log_json_path.json",
    use_partitioned_data="True",
    execution_date="{{ yesterday_ds }}"
)


stage_songs_to_redshift =  StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    target_table="public.staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_paths="",
    use_partitioned_data="False",
    execution_date="{{ yesterday_ds }}"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="public.songplays",
    sqlquery=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="public.users",
    sqlquery=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="public.songs",
    sqlquery=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="public.artists",
    sqlquery=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="public.time",
    sqlquery=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_list = ["public.songplays","public.users","public.songs","public.artists","public.time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift

stage_songs_to_redshift >> load_artist_dimension_table >> load_song_dimension_table

stage_events_to_redshift >> load_time_dimension_table
stage_events_to_redshift >> load_user_dimension_table

load_song_dimension_table >> load_songplays_table
load_time_dimension_table >> load_songplays_table
load_user_dimension_table >> load_songplays_table

load_songplays_table >> run_quality_checks >> end_operator