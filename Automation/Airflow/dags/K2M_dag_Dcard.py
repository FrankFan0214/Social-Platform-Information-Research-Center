# /opt/airflow/dags/kafka_consumer.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from tasks.dcard_consumer_task import kafka_consumer_job  # 匯入已分離的消費者任務函數
from utils.slack_webhook import slack_start_callback, slack_failure_callback, slack_success_callback

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_failure_callback,
    'on_success_callback': slack_success_callback,
}

with DAG(
    'K2M_dag_Dcard',
    default_args=default_args,
    description='A DAG to consume Kafka messages and store them in MongoDB',
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    # 程序啟動通知任務
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=slack_start_callback,
        provide_context=True # 自動將 context 傳入
    )
    consume_task = PythonOperator(
        task_id='consume_kafka_messages',
        python_callable=kafka_consumer_job,
        provide_context=True,
    )
    start_task >> consume_task