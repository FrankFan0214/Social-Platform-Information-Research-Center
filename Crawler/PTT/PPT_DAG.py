from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import requests
from bs4 import BeautifulSoup
import time
import json
from confluent_kafka import Producer
from datetime import datetime, timedelta
import gc

# 發送 Slack 通知的共用函數
def send_slack_message(context, message):
    # 取得當前任務的 log 網址並將 localhost 替換為外部 IP
    log_url = context.get('task_instance').log_url
    log_url = log_url.replace("localhost", "0.0.0.0")

    # 設定 Slack 通知內容，包括任務名稱、DAG 名稱、執行時間、以及 log 網址
    slack_msg = f"""
        {message}
        *Task*: {context.get('task_instance').task_id}  
        *Dag*: {context.get('task_instance').dag_id}  
        *Execution Time*: {context.get('execution_date')}  
        *Log URL*: {log_url}
    """

    # 使用時間戳來生成唯一的 Task ID，避免重複
    unique_task_id = f"slack_notification_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # 初始化 SlackWebhookOperator 來發送通知
    slack_notification = SlackWebhookOperator(
        task_id=unique_task_id,
        slack_webhook_conn_id='slack_webhook',
        message=slack_msg,
        dag=context['dag']
    )
    return slack_notification.execute(context=context)

# 程序啟動通知
def slack_start_callback(**kwargs):
    # 在程序啟動時發送通知
    context = kwargs['ti'].get_template_context()
    return send_slack_message(context, ":rocket: DAG 已啟動。")

# 任務失敗時的通知回調函數
def slack_failure_callback(context):
    return send_slack_message(context, ":red_circle: Task Failed.")

# 任務成功時的通知回調函數
def slack_success_callback(context):
    return send_slack_message(context, ":large_green_circle: Task Succeeded.")

# 爬蟲主程式，用於抓取 PTT 文章並將其資料發送至 Kafka
def frank_ptt(**context):
    max_pages = 3000  # 設定要爬取的最大頁數
    current_page = 1  # 記錄當前爬取的頁數

    # 用來接收從 Consumer instance 發出的錯誤訊息
    def error_cb(err):
        print('Error: %s' % err)

    # Kafka 連線設定
    kafka_config = {
        'bootstrap.servers': '104.155.214.8:9092',
        'max.in.flight.requests.per.connection': 1,
        'error_cb': error_cb
    }

    producer = Producer(kafka_config)
    topic = 'test-topic'  # 指定 Kafka topic 名稱

    base_url = "https://www.ptt.cc"
    board_url = "/bbs/Gossiping/index.html"
    cookies = {'over18': '1'}  # 設定 Cookie 來通過 PTT 年齡驗證

    # 設定 Kafka 傳送報告函數
    def delivery_report(err, msg):
        if err is not None:
            print(f"訊息傳送失敗: {err}")
        else:
            print(f"訊息成功傳送到 {msg.topic()} 分區 [{msg.partition()}]")

    # 取得文章內容的函數，並解析文章中的推文資訊
    def get_post_content(link):
        try:
            response = requests.get(link, cookies=cookies)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # 取得文章作者和推文資料
            content = soup.find('div', id='main-content')
            meta_info = soup.find_all('span', class_='article-meta-value')
            author = meta_info[0].text.strip() if len(meta_info) > 0 else "未知"

            # 解析推文的數量及詳細內容
            pushes = soup.find_all('div', class_='push')
            push_count, boo_count, arrow_count = 0, 0, 0
            comments = []

            for push in pushes:
                push_tag_element = push.find('span', class_='push-tag')
                if push_tag_element:
                    push_tag = push_tag_element.text.strip()
                else:
                    break  
                push_user = push.find('span', class_='push-userid').text.strip()
                push_content = push.find('span', class_='push-content').text.strip()
                push_time = push.find('span', class_='push-ipdatetime').text.strip()

                # 計算推文數量
                if push_tag == '推':
                    push_count += 1
                elif push_tag == '噓':
                    boo_count += 1
                else:
                    arrow_count += 1

                comments.append({
                    "推文": push_tag,
                    "用戶": push_user,
                    "內容": push_content[2:],
                    "時間": push_time
                })

            # 移除不要的元素並返回文章資料
            if content:
                for tag in content.find_all(['span', 'div']):
                    tag.extract()
                return {
                    "作者": author,
                    "內容": content.text.strip(),
                    "推": push_count,
                    "噓": boo_count,
                    "箭頭": arrow_count,
                    "留言": comments
                }
        except requests.RequestException as e:
            print(f"抓取文章內容時發生錯誤: {e}")
        return {}

    # 日期格式轉換函數
    def convert_date(date_str):
        try:
            return datetime.strptime(date_str, '%m/%d')
        except ValueError as e:
            print(f"日期轉換失敗: {e}")
            return {}

    # 主要爬取迴圈，逐頁抓取文章列表中的每篇文章
    while current_page < max_pages:
        try:
            response = requests.get(base_url + board_url, cookies=cookies)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            posts = soup.find_all('div', class_='r-ent')

            for post in posts:
                a_tag = post.find('a')
                if a_tag:
                    title = a_tag.text.strip()
                    link = base_url + a_tag['href']
                    post_data = get_post_content(link)
                    date_str = post.find('div', class_='date').text.strip()

                    # 如果文章有內容，則將資料轉換為 JSON 格式並傳送至 Kafka
                    if post_data:
                        post_document = {
                            "發佈日期": date_str,
                            "標題": title,
                            "作者": post_data["作者"],
                            "內容": post_data["內容"],
                            "推": post_data["推"],
                            "噓": post_data["噓"],
                            "箭頭": post_data["箭頭"],
                            "連結": link,
                            "留言": post_data["留言"]
                        }

                        json_data = json.dumps(post_document, ensure_ascii=False)
                        producer.produce(topic, key=link, value=json_data.encode('utf-8'), callback=delivery_report)
                        producer.flush()

                        print(f"已傳送文章至 Kafka: {title}")

            # 取得下一頁的連結
            btn_group = soup.find('div', class_='btn-group btn-group-paging')
            prev_link = btn_group.find_all('a')[1]['href']
            board_url = prev_link

            print(f"第{current_page}頁完成")
            del soup, posts, response, btn_group, prev_link
            gc.collect()
            current_page += 1

            # 每爬取 200 頁發送 Slack 通知
            if current_page % 200 == 0:
                def slack_page_callback(context):
                    slack_msg = f":memo: 已經爬取了 {current_page} 頁網站文章。"
                    send_slack_message(context=context, message=slack_msg)
                slack_page_callback(context)

            time.sleep(1)
        except requests.RequestException as e:
            print(f"抓取頁面時發生錯誤: {e}")
            return {}


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
    'C2k_dag_Ptt_Frank',
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
        python_callable=frank_ptt
    )
    start_task >> run_requests_task #任務執行順序