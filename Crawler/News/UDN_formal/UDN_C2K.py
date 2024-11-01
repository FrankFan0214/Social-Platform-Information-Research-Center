from selenium import webdriver
import requests
from utils.scroller import PageScroller
from utils.kafka_producer import KafkaProducer
from bs4 import BeautifulSoup
import time
from datetime import datetime
import json

# 初始化 KafkaProducer 實例
kafka_producer_news = KafkaProducer(
    servers='<IP>:9092', 
    topic='news-topic'
)

# Initialize WebDriver (e.g., Chrome) and PageScroller
driver = webdriver.Chrome()
scroller = PageScroller(driver, url="https://udn.com/news/breaknews/", max_scrolls=4, scroll_pause=1.5)

# Open the page and scroll
scroller.open_page()
scroller.scroll()

# 取得網頁的 HTML
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')

# 找到所有新聞項目
elements = soup.find_all('div', class_='story-list__text')  # 使用正確的 class 名稱

# 當前抓取的時間
crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 遍歷每個新聞項目
for element in elements:
    a_tag = element.find('a')
    if a_tag:
        # 安全地抓取新聞標題和連結
        title = a_tag.get('title', 'No title available')  # 使用 get 方法，如果 title 不存在，則設置為預設值
        # 跳過沒有標題的新聞項目
        if title == 'No title available':
            continue
        
        link = "https://udn.com" + a_tag.get('href', '')  # 構建完整的 URL，預設為空字串

        print(f"Title: {title}")
        print(f"Link: {link}")

        # 使用 requests 訪問每篇新聞的詳細內容頁面
        if link:  # 確保 link 不為空
            response = requests.get(link)
            article_soup = BeautifulSoup(response.content, 'html.parser')

            # 抓取 section class="article-content__wrapper" 中的內容
            article_content_div = article_soup.find('section', class_='article-content__wrapper')
            if article_content_div:
                # 抓取作者名稱
                author_tag = article_content_div.find('span', class_='article-content__author')
                reporter_name = author_tag.find('a').text.strip() if author_tag and author_tag.find('a') else "None"
                #抓取分類
                breadcrumb_items = article_content_div.find_all('a', class_='breadcrumb-items')
                if len(breadcrumb_items) >= 2:
                    # 取得後兩個項目的文字內容
                    categories = "/".join([item.get_text(strip=True) for item in breadcrumb_items[-2:]])
                # 抓取日期
                date_tag = article_content_div.find('time', class_='article-content__time')
                date = date_tag.text.strip() if date_tag else "None"

                # 抓取文章內容並去除所有空白和換行
                paragraphs = article_content_div.find_all('p')
                text_content = "".join(p.get_text(strip=True).replace(" ", "").replace("\n", "") for p in paragraphs)
                news_data = {
                    "title": title,
                    "url": link,
                    "reporter": reporter_name,
                    "content": text_content,
                    "date": date,
                    "news_category": categories,
                    "crawl_time": crawl_time
                }
                json_data = json.dumps(news_data, ensure_ascii=False)

                # 發送新聞資料至 Kafka
                kafka_producer_news.send_message(message=json_data.encode('utf-8'), key=link)
                print(f"已傳送文章至 Kafka: {title}")
            else:
                print("None")
                
# 清空緩衝區，確保所有訊息已發送至 Kafka
kafka_producer_news.flush()

# 關閉 KafkaProducer 連接
kafka_producer_news.close()

# 關閉瀏覽器
driver.quit()