from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (
    CreateTablesOperator,
    CleanAndUploadDataOperator, UploadDataOperator,
    StageToRedshiftOperator, LoadFactOperator,
    LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Rishabh Srivastava',
    'start_date': datetime.now(),
    'catchup': False,
    'depends_on_past': False
}

dag = DAG(
    'pollution_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = CreateTablesOperator(
    task_id='Create_tables',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_queries=[
        SqlQueries.drop_staging_pollution, SqlQueries.drop_staging_metar, 
        SqlQueries.drop_cities, SqlQueries.drop_facts, SqlQueries.drop_time,
        SqlQueries.create_staging_pollution, SqlQueries.create_staging_metar,
        SqlQueries.create_cities, SqlQueries.create_facts, SqlQueries.create_time
    ],
    dag=dag
)

clean_and_upload_pollution_data = CleanAndUploadDataOperator(
    task_id='Clean_and_upload_pollution_data',
    input_path="../pollution_data_final.json",
    aws_credentials_id="aws_credentials",
    s3_bucket="rish-dend-capstone",
    s3_key="pollution.csv",
    dag=dag
)

clean_and_upload_metar_data = CleanAndUploadDataOperator(
    input_path="../metar_data_final.json",
    task_id='Clean_and_upload_metar_data',
    aws_credentials_id="aws_credentials",
    s3_bucket="rish-dend-capstone",
    s3_key="metar.csv",
    dag=dag
)

load_and_copy_cities = UploadDataOperator(
    task_id='Load_and_copy_cities',
    input_path="../cities.csv",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="rish-dend-capstone",
    s3_key="cities.csv",
    table="cities",
    columns="city, state, latitude, longitude",
    dag=dag
)

stage_pollution_to_redshift = StageToRedshiftOperator(
    task_id='Stage_pollution',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_pollution",
    columns="city, metric, dt, value",
    s3_path='s3://rish-dend-capstone/pollution.csv',
    dag=dag
)

stage_weather_to_redshift = StageToRedshiftOperator(
    task_id='Stage_weather',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_metar",
    columns="city, metric, dt, value",
    s3_path='s3://rish-dend-capstone/metar.csv',
    dag=dag
)

load_facts_table = LoadFactOperator(
    task_id='Load_facts_fact_table',
    redshift_conn_id="redshift",
    table="facts",
    sql_source=SqlQueries.facts_table_insert,
    operation_type="truncate_then_append",
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    sql_source=SqlQueries.time_table_insert,
    operation_type="truncate_then_append",
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    dq_checks=[
        {"check_sql": f"SELECT COUNT(*) FROM facts;", "expected_result_type": "has_values"},
        {"check_sql": f"SELECT COUNT(*) FROM cities;", "expected_result_type": "has_values"},
        {"check_sql": f"SELECT COUNT(*) FROM time;", "expected_result_type": "has_values"}
    ],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> clean_and_upload_pollution_data
create_tables >> clean_and_upload_metar_data
create_tables >> load_and_copy_cities
clean_and_upload_pollution_data >> stage_pollution_to_redshift
clean_and_upload_metar_data >> stage_weather_to_redshift
stage_pollution_to_redshift >> load_facts_table
stage_weather_to_redshift >> load_facts_table
load_and_copy_cities >> load_facts_table
load_facts_table >> load_time_dimension_table
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator