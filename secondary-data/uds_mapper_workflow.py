
import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 5, 2)
}

staging_dataset = 'uds_mapper_workflow_staging'
modeled_dataset = 'uds_mapper_workflow_modeled'
table_name = 'zip_to_zcta5'
staging_name = staging_dataset + '.' + table_name
modeled_name = modeled_dataset + '.' + table_name

bq_query_start = 'bq query --use_legacy_sql=false '

create_mapper_sql = 'create or replace table ' + modeled_name + ' as \
                                select ZIP_CODE as ZIPCODE, STATE, cast(ZCTA as STRING) as ZCTA5\
                                from ' + staging_name +
                                ' where ZCTA is not NULL order by ZIPCODE'


with models.DAG(
        'uds_mapper_workflow',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging = BashOperator(
        task_id='create_staging_dataset',
        bash_command='bq --location=US mk --dataset ' + staging_dataset)

    create_modeled = BashOperator(
        task_id='create_modeled_dataset',
        bash_command='bq --location=US mk --dataset ' + modeled_dataset)

    load_mapper = BashOperator(
        task_id='load_mapper',
        bash_command='!bq --location=US load --autodetect --null_marker="No ZCTA" --skip_leading_rows=1 --allow_quoted_newlines \
--source_format=CSV ' + staging_name + ' "gs://uds-mapper/zip_to_zcta_2019.csv"',
        trigger_rule='one_success')

    create_mapper = BashOperator(
        task_id='create_mapper',
        bash_command=bq_query_start + "'" + create_mapper_sql + "'", 
        trigger_rule='one_success')
    
    mapper = BashOperator(
        task_id='mapper',
        bash_command='python /home/jupyter/airflow/dags/zip_to_zcta5_beam_dataflow.py')

create_staging >> create_modeled >> load_mapper >> create_mapper >> mapper
