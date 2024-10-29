from airflow.providers.http.hooks.http import HttpHook
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

    # 使用 HttpHook 發送 Slack 通知
    http_hook = HttpHook(http_conn_id='slack_webhook2', method='POST')
    response = http_hook.run(endpoint='', json={'text': slack_msg})

    if response.status_code != 200:
        raise ValueError(f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}")


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
