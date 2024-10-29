import requests
from bs4 import BeautifulSoup
import time
import json
from confluent_kafka import Producer
from datetime import date, datetime, timedelta
import gc

times = 0

# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)

# Kafka 設定
kafka_config = {
    # Kafka集群在那裡?
    'bootstrap.servers': '104.155.214.8:9092',  # <-- 置換成要連接的Kafka集群
    'max.in.flight.requests.per.connection': 1, 
    'error_cb': error_cb  # 設定接收error訊息的callback函數
}

producer = Producer(kafka_config)
topic = 'test-topic'  # 替換為你的 Kafka topic 名稱

base_url = "https://www.ptt.cc"
board_url = "/bbs/Gossiping/index.html"
cookies = {'over18': '1'}

def delivery_report(err, msg):
    """回報訊息傳送狀態"""
    if err is not None:
        print(f"訊息傳送失敗: {err}")
    else:
        print(f"訊息成功傳送到 {msg.topic()} 分區 [{msg.partition()}]")

def get_post_content(link):
    try:
        response = requests.get(link, cookies=cookies)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        content = soup.find('div', id='main-content')
        meta_info = soup.find_all('span', class_='article-meta-value')
        author = meta_info[0].text.strip() if len(meta_info) > 0 else "未知"

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

# 取得今天的日期
today = date.today()

# 計算前一天的日期
yesterday = today - timedelta(days=1)

# 格式化日期為 "MM/DD" 格式
formatted_date = yesterday.strftime("%m/%d")

stop_date = datetime.strptime(formatted_date,"%m/%d")

while times <= 10:
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
                post_date = convert_date(date_str)

                # 停止抓取條件
                if  post_date < stop_date:
                    times = times + 1
                    if times > 10:
                        print(f"評估後續文章已晚於前一日，自動終止抓取")
                        break
                    else:
                        break
                

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

                    # 將文章資料轉換為 JSON 格式
                    json_data = json.dumps(post_document, ensure_ascii=False)

                    # 將資料傳送到 Kafka
                    producer.produce(topic, key=link, value=json_data.encode('utf-8'), callback=delivery_report)
                    producer.flush()  # 確保訊息已傳送

                    print(f"已傳送文章至 Kafka: {title}")

        btn_group = soup.find('div', class_='btn-group btn-group-paging')
        prev_link = btn_group.find_all('a')[1]['href']
        board_url = prev_link
        del soup, posts, response, btn_group, prev_link
        gc.collect()  # 強制執行垃圾回收
        time.sleep(10)
    except requests.RequestException as e:
        print(f"抓取頁面時發生錯誤: {e}")
        break
