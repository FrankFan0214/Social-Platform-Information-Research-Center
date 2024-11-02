from selenium import webdriver
import requests
from utils.scroller import PageScroller
from utils.kafka_producer import KafkaProducer
from bs4 import BeautifulSoup
import time
from datetime import datetime
import json
from time import sleep
import re

# 初始化 KafkaProducer 實例
kafka_producer_news = KafkaProducer(
    servers='<IP>:9092', 
    topic='news-topic'
)

# Initialize WebDriver (e.g., Chrome) and PageScroller
driver = webdriver.Chrome()
scroller = PageScroller(driver, url="https://money.udn.com/rank/newest/1001/0", max_scrolls=4, scroll_pause=1.5)

# Open the page and scroll
scroller.open_page()
# 等待頁面部分加載
sleep(5)

scroller.scroll()

# 取得網頁的 HTML
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')

# 找到所有新聞項目
elements = soup.find_all('div', class_='story__content')

# 當前抓取的時間
crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 遍歷每個新聞項目
for element in elements:
    a_tag = element.find('a')
    if a_tag:
        # 提取日期時間
        date_tag = element.find('time')
        date_text = date_tag.text if date_tag else "未知"
        
        # 格式化日期時間
        formatted_date = "未知"
        if date_text != "未知":
            try:
                formatted_date = datetime.strptime(date_text, "%Y-%m-%d %H:%M").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
        print(f"Date: {formatted_date}")
        
        # 提取標題
        title = a_tag.get('title', '未知')
        
        # 提取完整的連結
        link = a_tag.get('href', '未知')

        # # 提取內文摘要
        # content_tag = element.find('p', class_='story__text')
        # content = content_tag.text.strip() if content_tag else "未知"

        print(f"Title: {title}")
        print(f"Link: {link}")
        # print(f"Summary: {content}")

        # 使用 requests 訪問每篇新聞的詳細內容頁面
        if link != "未知":  # 確保 link 不為空
            response = requests.get(link)
            article_soup = BeautifulSoup(response.content, 'html.parser')

            # 抓取文章的主要內容
            article_content_div = article_soup.find('main', class_='article-main-content')
            if article_content_div:
                # 抓取作者名稱
                author_tag = article_content_div.find('div', class_='article-body__info')
                author_info = author_tag.find('span').text.strip() if author_tag and author_tag.find('span') else "None"
                # 匹配「記者」或「編譯」後接上兩到四個中文字符的姓名
                reporter_match = re.search(r"(記者|編譯)([\u4e00-\u9fa5]{2,3})", author_info)

                # 提取記者或編譯者的姓名
                reporter_name = reporter_match.group(2) if reporter_match else "未知"
                print(f"Author: {reporter_name}")

                # 抓取分類
                breadcrumb_items = article_content_div.find_all('li', class_='breadcrumb__item')
                categories = "None"
                if len(breadcrumb_items) >= 2:
                    categories = "/".join([item.get_text(strip=True) for item in breadcrumb_items[-2:]])
                    print(f"Category: {categories}")
                else:
                    print("Not enough breadcrumb items found.")

                # 抓取文章內容並保留自然段落
                paragraphs = article_content_div.find('section',id='article_body').find_all('p')             
                # paragraphs = paragraphs.find_all('p')
                text_content = "".join(p.get_text(strip=True).replace(" ", "").replace("\n", "") for p in paragraphs)
                
                # 建立新聞資料字典，新增分類欄位
                news_data = {
                    "title": title,
                    "url": link,
                    "reporter": reporter_name,
                    "content": text_content,
                    "date": formatted_date,
                    "category": categories,
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