from selenium import webdriver
import requests
from utils.scroller import PageScroller
from utils.kafka_producer import KafkaProducer
from bs4 import BeautifulSoup
import time
from datetime import datetime
import json
import re

# 初始化 KafkaProducer 實例
kafka_producer_news = KafkaProducer(
    servers='104.155.214.8:9092', 
    topic='dcard-topic'
)

# 初始化 WebDriver 和 PageScroller
driver = webdriver.Chrome()
scroller = PageScroller(driver, url="https://www.setn.com/ViewAll.aspx", max_scrolls=20, scroll_pause=1.5)

# 開啟網頁並滾動
scroller.open_page()
scroller.scroll()

# 取得網頁的 HTML
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')

# 找到所有新聞項目
news_items = soup.find_all(class_='newsItems')

# 當前抓取的時間
crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 遍歷每個新聞項目，提取分類、完整的 href 和標題，並抓取詳細內容
for item in news_items:
    # 提取分類和標題
    news_category = item.find('div', class_='newslabel-tab').a.text
    news_link = 'https://www.setn.com/' + item.find('a', class_='gt')['href']
    news_title = item.find('a', class_='gt').text.strip()

    # 使用 requests 獲取每條新聞的詳細內容
    response = requests.get(news_link)
    news_soup = BeautifulSoup(response.content, 'html.parser')

    # 找到文章內容並提取所有文字
    article_content = news_soup.find('div', id='ckuse')
    if article_content:
        # 提取文章文字
        text_content = article_content.get_text(separator="\n", strip=True)
        
        # 提取發佈時間（假設發佈時間在 time 標籤內）
        publish_time_tag = news_soup.find('time', class_='page_date')
        publish_time = publish_time_tag.text.strip() if publish_time_tag else "未知"

        # 使用正規表達式匹配記者姓名
        reporter_match = re.search(r"記者([\u4e00-\u9fa5]{2,4})／", text_content)
        reporter_name = reporter_match.group(1) if reporter_match else "未知"

        # 建立新聞資料字典，新增分類欄位
        news_data = {
            "title": news_title,
            "url": news_link,
            "reporter": reporter_name,
            "content": text_content,
            "date": publish_time,
            "news_category": news_category,
            "crawl_time": crawl_time
        }
        json_data = json.dumps(news_data, ensure_ascii=False)

        # 發送新聞資料至 Kafka
        kafka_producer_news.send_message(message=json_data.encode('utf-8'), key=news_link)
        print(f"已傳送文章至 Kafka: {news_title}")
    else:
        print(f"找不到 id='ckuse' 的內容，跳過該新聞：{news_link}")

# 清空緩衝區，確保所有訊息已發送至 Kafka
kafka_producer_news.flush()

# 關閉瀏覽器
driver.quit()