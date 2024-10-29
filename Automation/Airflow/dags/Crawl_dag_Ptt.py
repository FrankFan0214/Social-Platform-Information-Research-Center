from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.slack_webhook_ptt import send_slack_message,slack_start_callback, slack_failure_callback, slack_success_callback
from datetime import datetime, timedelta
from tasks.ptt_crawl_producer import C2K_ptt
# 設置默認參數
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_failure_callback,  # 失敗時發送 Slack 通知
    'on_success_callback': slack_success_callback,  # 成功時發送 Slack 通知
}

# 定義 DAG
with DAG(
    'C2K_dag_Ptt',
    default_args=default_args,
    description='A simple ptt DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    # 程序啟動通知任務
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=slack_start_callback,
        provide_context=True # 自動將 context 傳入
    )
    # 爬取 PTT 文章任務
    run_requests_task = PythonOperator(
        task_id='run_requests_task',
        python_callable=C2K_ptt
    )
    start_task >> run_requests_task