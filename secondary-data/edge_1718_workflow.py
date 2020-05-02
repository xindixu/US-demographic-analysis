
import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 5, 1)
}


staging_dataset = 'edge_1718_workflow_staging'
modeled_dataset = 'edge_1718_workflow_modeled'

ps_table_name = 'postsecondary_school'
pu_table_name = 'public_school'
pr_table_name = 'private_school'

ps_staging_name = staging_dataset + '.' + ps_table_name
ps_modeled_name = modeled_dataset + '.' + ps_table_name

pu_staging_name = staging_dataset + '.' + pu_table_name
pu_modeled_name = modeled_dataset + '.' + pu_table_name

pr_staging_name = staging_dataset + '.' + pr_table_name
pr_modeled_name = modeled_dataset + '.' + pr_table_name

bq_query_start = 'bq query --use_legacy_sql=false '

create_postsecondary_sql = 'create or replace table ' + ps_modeled_name + ' as \
                                select UNITID as SCHOOLID, NAME, STREET, CITY, STATE, ZIP as ZIPCODE, \
                                cast(LAT as FLOAT64) as LATITUDE, cast(LON as FLOAT64) as LONGITUDE \
                                from ' + ps_staging_name + ' order by ZIPCODE'

create_public_sql = 'create or replace table ' + pu_modeled_name + ' as \
                                select NCESSCH as SCHOOLID, NAME, STREET, CITY, STATE, ZIP as ZIPCODE, \
                                cast(LAT as FLOAT64) as LATITUDE, cast(LON as FLOAT64) as LONGITUDE \
                                from ' + pu_staging_name + ' order by ZIPCODE'

create_private_sql = 'create or replace table ' + pr_modeled_name + ' as \
                                select PPIN as SCHOOLID, PINST as NAME, STREET, CITY, STATE, ZIP as ZIPCODE, \
                                cast(LAT as FLOAT64) as LATITUDE, cast(LON as FLOAT64) as LONGITUDE \
                                from ' + pr_staging_name + ' order by ZIPCODE'


with models.DAG(
        'edge_1718_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging = BashOperator(
        task_id='create_staging_dataset',
        bash_command='bq --location=US mk --dataset ' + staging_dataset)

    create_modeled = BashOperator(
        task_id='create_modeled_dataset',
        bash_command='bq --location=US mk --dataset ' + modeled_dataset)
    
    branch = DummyOperator(
            task_id='branch',
            trigger_rule='all_done')

    load_postsecondary = BashOperator(
        task_id='load_postsecondary',
        bash_command=
        'bq --location=US load --skip_leading_rows=1 \
        --source_format=CSV ' + ps_staging_name + ' \
        "gs://edge-geocode-1718/EDGE_GEOCODE_POSTSECSCH_1718.csv" \
        /home/jupyter/airflow/dags/edge_schema.json',
        trigger_rule='one_success')
    
    load_public = BashOperator(
        task_id='load_public',
        bash_command=
        'bq --location=US load --skip_leading_rows=1 \
        --source_format=CSV ' + pu_staging_name + ' \
        "gs://edge-geocode-1718/EDGE_GEOCODE_PUBLICSCH_1718.csv" \
        /home/jupyter/airflow/dags/edge_public_schema.json',
        trigger_rule='one_success')
    
    load_private = BashOperator(
        task_id='load_private',
        bash_command='bq --location=US load --skip_leading_rows=1 \
        --source_format=CSV ' + pr_staging_name + ' \
        "gs://edge-geocode-1718/EDGE_GEOCODE_PRIVATESCH_1718.csv" \
        /home/jupyter/airflow/dags/edge_private_schema.json',
        trigger_rule='one_success')

    create_postsecondary = BashOperator(
        task_id='create_postsecondary',
        bash_command=bq_query_start + "'" + create_postsecondary_sql + "'", 
        trigger_rule='one_success')
    
    create_public = BashOperator(
        task_id='create_pulic',
        bash_command=bq_query_start + "'" + create_public_sql + "'", 
        trigger_rule='one_success')
    
    create_private = BashOperator(
        task_id='create_private',
        bash_command=bq_query_start + "'" + create_private_sql + "'", 
        trigger_rule='one_success')
    
    beam_postsecondary = BashOperator(
        task_id='beam_postsecondary',
        bash_command='python /home/jupyter/airflow/dags/postsecondary_school_beam_dataflow_airflow.py',
        trigger_rule='one_success')
    
    beam_public = BashOperator(
        task_id='beam_public',
        bash_command='python /home/jupyter/airflow/dags/public_school_beam_dataflow_airflow.py',
        trigger_rule='one_success')
    
    beam_private = BashOperator(
        task_id='beam_private',
        bash_command='python /home/jupyter/airflow/dags/private_school_beam_dataflow_airflow.py',
        trigger_rule='one_success')


    
    
    create_staging >> create_modeled >> branch
    branch >> load_postsecondary >> create_postsecondary >> beam_postsecondary
    branch >> load_public >> create_public >> beam_public
    branch >> load_private >> create_private >> beam_private

