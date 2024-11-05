import os
import subprocess
import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from utils.scroller import PageScroller
from utils.kafka_producer import KafkaProducer

# 啟動 Xvfb，並指定顯示編號 :99
xvfb_process = subprocess.Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24'])
os.environ["DISPLAY"] = ":99"

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
        options.headless = headless
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920x1080")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.scroller = PageScroller(self.driver, url="https://udn.com/news/breaknews/", max_scrolls=max_scrolls, scroll_pause=scroll_pause)
        self.kafka_producer = KafkaProducer(servers=kafka_servers, topic=kafka_topic)
        self.crawl_time = datetime.now().strftime('%Y-%m-%d')

    def open_and_scroll_page(self):
        """Open the page and perform scrolling."""
        self.scroller.open_page()
        self.scroller.scroll()

    def fetch_news_items(self):
        """Fetch all news items from the page."""
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        return soup.find_all('div', class_='story-list__text')

    def parse_news_item(self, element):
        """Parse a single news item to extract information."""
        a_tag = element.find('a')
        if not a_tag:
            return None

        title = a_tag.get('title', 'No title available')
        if title == 'No title available':
            return None
        
        href = a_tag.get('href', '')
        link = href if href.startswith("http") else f"https://udn.com{href}"

        # Fetch and parse the news article content
        response = requests.get(link)
        article_soup = BeautifulSoup(response.content, 'html.parser')
        article_content_div = article_soup.find('section', class_='article-content__wrapper')

        if not article_content_div:
            return None

        # Extract reporter name
        author_tag = article_content_div.find('span', class_='article-content__author')
        reporter_name = author_tag.find('a').text.strip() if author_tag and author_tag.find('a') else "None"

        # Extract categories
        categories = "未分類"
        breadcrumb_items = article_content_div.find_all('a', class_='breadcrumb-items')
        if len(breadcrumb_items) >= 2:
            categories = "/".join([item.get_text(strip=True) for item in breadcrumb_items[-2:]])

        # Extract date
        date_tag = article_content_div.find('time', class_='article-content__time')
        date_text = date_tag.text.strip() if date_tag else "None"
        formatted_date_str = "未知"

        if date_text != "None":
            try:
                parsed_date = datetime.strptime(date_text, "%Y-%m-%d %H:%M")
                formatted_date_str = parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                print("日期格式無法解析")

        # Extract article content
        paragraphs = article_content_div.find_all('p')
        text_content = "".join(p.get_text(strip=True).replace(" ", "").replace("\n", "") for p in paragraphs)
        
        return NewsItem(
            title=title,
            url=link,
            reporter=reporter_name,
            content=text_content,
            date=formatted_date_str,
            category=categories,
            crawl_time=self.crawl_time
        )

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

        self.kafka_producer.flush()
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
        max_scrolls = 4
        headless = True

        scraper = NewsScraper(kafka_servers, kafka_topic, max_scrolls=max_scrolls, headless=headless)
        scraper.scrape_and_publish()
    finally:
        xvfb_process.terminate()