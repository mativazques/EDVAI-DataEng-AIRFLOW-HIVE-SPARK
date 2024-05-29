from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'sqoop-airflow',
    default_args=default_args,
    description='ETL. Ingest: Sqoop. Transform: Spark.',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['edvai'],
) as dag:   
    comienza_proceso = EmptyOperator(
        task_id='comienza_proceso',
    )

    finaliza_proceso = EmptyOperator(
        task_id='finaliza_proceso',
    )

    with TaskGroup(group_id='Ingest') as Ingest:        
        extract_table_1 = BashOperator(
            task_id='extract_table_1',
            bash_command='/usr/bin/sh /home/hadoop/scripts/clase9/ingest/extract_table_1.sh ',
        )

        extract_table_2 = BashOperator(
            task_id='extract_table_2',
            bash_command='/usr/bin/sh /home/hadoop/scripts/clase9/ingest/extract_table_2.sh ',
        )

        extract_table_3 = BashOperator(ca
            task_id='extract_table_3',
            bash_command='/usr/bin/sh /home/hadoop/scripts/clase9/ingest/extract_table_3.sh ',
        )
        
    with TaskGroup('Process') as Process:
        processing_table_1 = BashOperator(
            task_id='processing_table_1',
            bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/clase9/transform/processing_table_1.py',
        )

        processing_table_2_3 = BashOperator(
            task_id='processing_table_2_3',
            bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/clase9/transform/processing_table_2_3.py',
        )

    comienza_proceso >> Ingest >> Process >> finaliza_proceso