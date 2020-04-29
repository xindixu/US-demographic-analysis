
import datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_dag_args = {
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 29)
}

staging_dataset = 'iml_2018_workflow_staging'
modeled_dataset = 'iml_2018_workflow_modeled'
table_name = 'Public_Resources'
staging_name = staging_dataset + '.' + table_name
modeled_name = modeled_dataset + '.' + table_name

bq_query_start = 'bq query --use_legacy_sql=false '

create_public_resources_sql = 'create or replace table ' + modeled_name + ' as \
                                select MID, DISCIPL, COMMONNAME, LEGALNAME, ADSTREET, ADCITY, ADSTATE, \
                                cast(ADZIP5 as STRING) as ZIPCODE, \
                                PHONE, WEBURL, \
                                cast(replace(INCOME15, ' ', '0') as FLOAT64) as INCOME, \
                                cast(replace(REVENUE15, ' ', '0') as FLOAT64) as REVENUE, \
                                LONGITUDE, LATITUDE\
                                from ' + staging_name


with models.DAG(
        'college_workflow1',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    create_staging = BashOperator(
        task_id='create_staging_dataset',
        bash_command='bq --location=US mk --dataset ' + staging_dataset)

    create_modeled = BashOperator(
        task_id='create_modeled_dataset',
        bash_command='bq --location=US mk --dataset ' + modeled_dataset)

    load_public_resources = BashOperator(
        task_id='load_public_resources',
        bash_command='bq --location=US load --autodetect --allow_quoted_newlines --allow_jagged_rows --ignore_unknown_values \
                         --source_format=CSV ' + staging_name + ' "gs://iml-2018/public_resources.csv',
        trigger_rule='one_success')

    create_public_resources = BashOperator(
        task_id='create_public_resources',
        bash_command=bq_query_start + "'" + create_public_resources_sql + "'", 
        trigger_rule='one_success')

create_staging >> create_modeled >> load_public_resources >> create_public_resources 
