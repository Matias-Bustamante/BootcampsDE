from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='etl_formula1',
    default_args=args,
    description='Realiza proceso ETL de formula 1',
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
    params={"example_key": "example_value"},
) as dag:

    finaliza_proceso = DummyOperator(
        task_id='finaliza_proceso',
    )

    


    ingest = BashOperator(
        task_id='ingesta',
        bash_command='/usr/bin/sh /home/hadoop/scripts/Ejercicio8/Ingest.sh ',
    )

    transformacion = BashOperator(
        task_id='transformacion',
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/Ejercicio8/formula1.py ',
    )

    
    ingest >> transformacion >>  finaliza_proceso




if __name__ == "__main__":
    dag.cli()