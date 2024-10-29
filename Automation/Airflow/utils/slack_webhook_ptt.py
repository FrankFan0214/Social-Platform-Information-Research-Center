from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime

# 發送 Slack 通知的共用函數
def send_slack_message(context, message):
    log_url = context.get('task_instance').log_url
    log_url = log_url.replace("localhost", "35.221.244.222")  # 替換 localhost

    slack_msg = f"""
        {message}
        *Task*: {context.get('task_instance').task_id}  
        *Dag*: {context.get('task_instance').dag_id}  
        *Execution Time*: {context.get('execution_date')}  
        *Log URL*: {log_url}
    """

    # 使用時間戳來生成唯一的 Task ID
    unique_task_id = f"slack_notification_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    slack_notification = SlackWebhookOperator(
        task_id=unique_task_id,
        slack_webhook_conn_id='slack_webhook',
        message=slack_msg,
        dag=context['dag']
    )
    return slack_notification.execute(context=context)

# 程序啟動通知
def slack_start_callback(**kwargs):
    context = kwargs['ti'].get_template_context()
    return send_slack_message(context, ":rocket: DAG 已啟動。")

# 失敗回調函數
def slack_failure_callback(context):
    return send_slack_message(context, ":red_circle: Task Failed.")

# 成功回調函數
def slack_success_callback(context):
    return send_slack_message(context, ":large_green_circle: Task Succeeded.")