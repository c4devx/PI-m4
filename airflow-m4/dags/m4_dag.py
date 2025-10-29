from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

SSH_CONN_ID = "spark-connection"
SPARK_PROJECT_PATH = "/home/ubuntu/spark-project"

with DAG(
    dag_id='dept01_m4_etlt',
    start_date=datetime(2025, 1, 1),
    schedule_interval=timedelta(days=1), 
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
        'ssh_conn_id': SSH_CONN_ID, 
    }
) as dag:
    
    run_silver = SSHOperator(
        task_id='run_silver',
        command=cmd_silver,
        dag=dag,
    )

    run_gold = SSHOperator(
        task_id='run_gold',
        command=cdm_gold,
        dag=dag,
    )

    run_silver >> run_gold