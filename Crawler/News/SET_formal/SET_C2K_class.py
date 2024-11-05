import os
import subprocess

# 啟動 Xvfb，並指定顯示編號 :99
xvfb_process = subprocess.Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24'])
os.environ["DISPLAY"] = ":99"

import sys
import json
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import requests
from webdriver_manager.chrome import ChromeDriverManager
from utils.scroller import PageScroller
from utils.kafka_producer import KafkaProducer

class NewsItem:
    """Represents a news item with its attributes."""
    
    def __init__(self, title, url, reporter, content, date, category, crawl_time):
        self.title = title
        self.url = url
        self.reporter = reporter
        self.content = content
        self.date = date
        self.category = category
        self.crawl_time = crawl_time

    def to_json(self):
        """Convert the news item to JSON format."""
        return json.dumps(self.__dict__, ensure_ascii=False)

class NewsScraper:
    """Handles the scraping of news items and sending to Kafka."""

    def __init__(self, kafka_servers, kafka_topic, max_scrolls=4, scroll_pause=1.5, headless=True):
        # 初始化 WebDriver 選項
        options = Options()
        options.headless = headless  # 啟用無頭模式
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")  # 沙盒模式
        options.add_argument("--disable-dev-shm-usage")  # 避免資源不足
        options.add_argument("--remote-debugging-port=9222")  # 遠程調試端口
        options.add_argument("--disable-software-rasterizer")  # 禁用軟件光柵化
        options.add_argument("--window-size=1920x1080")  # 設定視窗大小

        service = Service(ChromeDriverManager().install())
        
        # 將 driver 定義為類別屬性
        self.driver = webdriver.Chrome(service=service, options=options)
        
        # 初始化 Kafka Producer 和其他屬性
        self.kafka_producer = KafkaProducer(servers=kafka_servers, topic=kafka_topic)
        self.max_scrolls = max_scrolls  # 將滑動次數設為物件屬性
        self.scroller = PageScroller(self.driver, url="https://www.setn.com/ViewAll.aspx", max_scrolls=self.max_scrolls, scroll_pause=scroll_pause)
        self.crawl_time = datetime.now().strftime('%Y-%m-%d')

    def open_and_scroll_page(self):
        """Open the page and perform scrolling."""
        try:
            self.scroller.open_page()
            self.scroller.scroll()
        except Exception as e:
            print(f"滾動時發生錯誤: {e}")
            self.cleanup()
            sys.exit(1)

    def fetch_news_items(self):
        """Fetch all news items from the page."""
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        return soup.find_all(class_='newsItems')

    def parse_news_item(self, item):
        """Parse a single news item to extract information."""
        try:
            # 提取基本信息
            news_category = item.find('div', class_='newslabel-tab').a.text
            news_link = 'https://www.setn.com/' + item.find('a', class_='gt')['href']
            news_title = item.find('a', class_='gt').text.strip()

            # 請求並解析詳細新聞內容
            response = requests.get(news_link)
            news_soup = BeautifulSoup(response.content, 'html.parser')
            article_content = news_soup.find('div', id='ckuse')
            if not article_content:
                print(f"找不到 id='ckuse' 的內容，跳過該新聞：{news_link}")
                return None

            text_content = article_content.get_text(separator="\n", strip=True)
            
            # 提取發佈時間
            publish_time_tag = news_soup.find('time', class_='page_date')
            publish_time = publish_time_tag.text.strip() if publish_time_tag else "未知"
            if publish_time != "未知":
                parsed_date = datetime.strptime(publish_time, "%Y/%m/%d %H:%M:%S")
                formatted_date_str = parsed_date.strftime("%Y-%m-%d")
            else:
                formatted_date_str = "未知"

            # 提取記者姓名
            reporter_match = re.search(r"記者([\u4e00-\u9fa5]{2,4})／", text_content)
            reporter_name = reporter_match.group(1) if reporter_match else "未知"

            # 建立新聞項目
            return NewsItem(
                title=news_title,
                url=news_link,
                reporter=reporter_name,
                content=text_content,
                date=formatted_date_str,
                category=news_category,
                crawl_time=self.crawl_time
            )
        except Exception as e:
            print(f"處理新聞項目時發生錯誤：{e}")
            return None

    def send_to_kafka(self, news_item):
        """Send news item to Kafka as a JSON message."""
        json_data = news_item.to_json()
        self.kafka_producer.send_message(message=json_data.encode('utf-8'), key=news_item.url)
        print(f"已傳送文章至 Kafka: {news_item.title}")

    def scrape_and_publish(self):
        """Main method to scrape news and publish them to Kafka."""
        self.open_and_scroll_page()
        news_items = self.fetch_news_items()

        for item in news_items:
            news_item = self.parse_news_item(item)
            if news_item:
                self.send_to_kafka(news_item)

        # 清空 Kafka 緩衝區並關閉連接
        self.kafka_producer.flush()
        self.cleanup()
        print("已完成所有新聞的抓取和傳送至 Kafka")

    def cleanup(self):
        """Clean up resources by closing Kafka producer and WebDriver."""
        self.kafka_producer.close()
        self.driver.quit()


# 主程式
if __name__ == "__main__":
    try:
        kafka_servers = '<IP>:9092'
        kafka_topic = 'news-topic'
        max_scrolls = 4  # 設定滑動次數
        headless = True  # 設定無頭模式（True 表示無頭，False 表示顯示瀏覽器窗口）

        scraper = NewsScraper(kafka_servers, kafka_topic, max_scrolls=max_scrolls, headless=headless)
        scraper.scrape_and_publish()
    
    finally:
        # 結束 Xvfb 虛擬顯示器
        xvfb_process.terminate()