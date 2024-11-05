import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple
import time
import json
from confluent_kafka import Producer
from datetime import date, datetime, timedelta
import gc

class ETTodayNewsScraper:
    def __init__(self, kafka_config: Dict = None, topic: str = None):
        self.base_url = "https://www.ettoday.net"
        self.news_categories = {
            "熱門": {
                "url": "https://www.ettoday.net/news/hot-news.htm",
                "selector": "body > div.wrapper_box > div > div.container_box > div > div > div.c1 > div > div.block_content > div > div:nth-child({}) > h3 > a",
                "max_items": 50
            },
            "生活": {
                "url": "https://www.ettoday.net/news/focus/%E7%94%9F%E6%B4%BB/",
                "selector": "#lifestyle > div.wrapper_box > div > div.container_box > div > div > div.c1 > div.block.block_1.infinite_scroll > div.block_content > div > div:nth-child({}) > h3 > a",
                "max_items": 20
            },
            "政治": {
                "url": "https://www.ettoday.net/news/focus/%E6%94%BF%E6%B2%BB/",
                "selector": "#news > div.wrapper_box > div > div.container_box > div > div > div.c1 > div.block.block_1.infinite_scroll > div.block_content > div > div:nth-child({}) > h3 > a",
                "max_items": 20
            },
            "焦點": {
                "url": "https://www.ettoday.net/news/focus/%E7%84%A6%E9%BB%9E%E6%96%B0%E8%81%9E/",
                "selector": "#newslist > div.wrapper_box > div > div.container_box > div > div > div.c1 > div.block.block_1.infinite_scroll > div.block_content > div > div:nth-child({}) > h3 > a",
                "max_items": 20
            },
            "3c": {
                "url": "https://www.ettoday.net/news/focus/3C%E5%AE%B6%E9%9B%BB/",
                "selector": "#teck3c > div.wrapper_box > div > div.container_box > div > div > div.c1 > div.block.block_1.infinite_scroll > div.block_content > div > div:nth-child({}) > h3 > a",
                "max_items": 20
            }
        }
        
        self.kafka_config = kafka_config or {
            'bootstrap.servers': '104.155.214.8:9092',
            'max.in.flight.requests.per.connection': 1,
            'error_cb': self.error_cb
        }
        self.topic = topic
        self.producer = Producer(self.kafka_config) if kafka_config is not None else None
        
        self.today = date.today()
        self.yesterday = self.today - timedelta(days=1)
        self.formatted_date = self.yesterday.strftime("%m/%d")
        self.stop_date = datetime.strptime(self.formatted_date, "%m/%d")

    @staticmethod
    def error_cb(err):
        print('Error: %s' % err)

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"訊息傳送失敗: {err}")
        else:
            print(f"訊息成功傳送到 {msg.topic()} 分區 [{msg.partition()}]")

    def extract_reporter(self, text: str) -> str:
        """從文字中提取記者名字"""
        if not text:
            return ""
        
        try:
            # 移除多餘的空白
            text = text.strip()
            
            # 尋找記者和報導之間的文字
            start_idx = text.find('記者')
            end_idx = text.find('／')
            
            if start_idx != -1 and end_idx != -1:
                reporter_name = text[start_idx + 2:end_idx].strip()
                # 清理記者名字中的特殊字符
                reporter_name = ''.join(char for char in reporter_name if char.isalnum() or char.isspace())
                return reporter_name.strip()
            return ""
        except Exception:
            return ""

    def send_to_kafka(self, news_data: Dict) -> None:
        if self.producer is None:
            return
        
        try:
            # 確保reporter欄位是字串類型
            if news_data.get('reporter') is None:
                news_data['reporter'] = ""
                
            # 序列化前檢查所有欄位
            cleaned_data = {
                'title': str(news_data.get('title', '')),
                'url': str(news_data.get('url', '')),
                'reporter': str(news_data.get('reporter', '')),
                'content': str(news_data.get('content', '')),
                'date': str(news_data.get('date', '')),
                'category': str(news_data.get('category', '')),
                'crawl_time': str(news_data.get('crawl_time', ''))
            }
            
            json_data = json.dumps(cleaned_data, ensure_ascii=False)
            self.producer.produce(
                self.topic,
                key=cleaned_data['url'],
                value=json_data.encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.flush()
            print(f"已傳送文章至 Kafka: {cleaned_data['title']}")
        except Exception as e:
            print(f"發送到Kafka時發生錯誤: {str(e)}")

    def _fetch_page_content(self, url: str) -> Optional[BeautifulSoup]:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            print(f"無法連接到頁面，狀態碼: {response.status_code}")
            return None
        except Exception as e:
            print(f"發生錯誤: {str(e)}")
            return None

    def _get_article_content_and_date(self, url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        soup = self._fetch_page_content(url)
        content = None
        date = None
        reporter = None
        
        if soup:
            # 獲取文章內容
            article_content = soup.find('div', class_='story')
            content = article_content.get_text(strip=True) if article_content else None
            
            # 獲取記者資訊 - 改進版本
            reporter_candidates = [
                soup.find('p', string=lambda x: x and '記者' in x and '／' in x),
                soup.find('p', class_='story_author'),
                soup.find('div', string=lambda x: x and '記者' in x and '／' in x)
            ]
            
            for candidate in reporter_candidates:
                if candidate:
                    reporter_text = candidate.get_text(strip=True)
                    reporter = self.extract_reporter(reporter_text)
                    if reporter:
                        break

            # 獲取日期
            date_element = soup.find('time', class_='date')
            date = date_element.get_text(strip=True) if date_element else None
                
        return content, date, reporter

    def fetch_homepage_news(self) -> List[Dict]:
        news_list = []
        soup = self._fetch_page_content(self.base_url)
        
        if not soup:
            return news_list

        titles = soup.find_all('h2', class_='title')
        for title in titles:
            link = title.find('a')
            if link:
                title_text = title.get_text(strip=True)
                article_url = link['href']
                if not article_url.startswith('http'):
                    article_url = self.base_url + article_url
                
                content, date, reporter = self._get_article_content_and_date(article_url)
                if content:
                    news_data = {
                        'title': title_text,
                        'url': article_url,
                        'reporter': reporter or "",
                        'content': content,
                        'date': date or "",
                        'category': 'homepage',
                        'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    news_list.append(news_data)
                    self.send_to_kafka(news_data)
                    gc.collect()
                    time.sleep(1)

        return news_list

    def fetch_news(self, category: str) -> List[Dict]:
        if category not in self.news_categories:
            raise ValueError(f"不支援的新聞類別: {category}")

        news_list = []
        category_info = self.news_categories[category]
        soup = self._fetch_page_content(category_info["url"])
        
        if not soup:
            return news_list

        for selector in range(1, category_info["max_items"] + 1):
            css_selector = category_info["selector"].format(selector)
            title_element = soup.select_one(css_selector)
            
            if title_element:
                title_text = title_element.get_text(strip=True)
                article_url = title_element.get('href', '')
                if not article_url.startswith('http'):
                    article_url = self.base_url + article_url
                
                content, date, reporter = self._get_article_content_and_date(article_url)
                if content:
                    news_data = {
                        'title': title_text,
                        'url': article_url,
                        'reporter': reporter or "",
                        'content': content,
                        'date': date or "",
                        'category': category,
                        'crawl_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    news_list.append(news_data)
                    self.send_to_kafka(news_data)
                    gc.collect()
                    time.sleep(1)
            else:
                print(f"無法找到符合的選擇器: {selector}")

        return news_list

    def print_news(self, news_list: List[Dict]) -> None:
        for idx, news in enumerate(news_list, 1):
            print(f'標題{idx}: {news["title"]}')
            print(f'網址: {news["url"]}')
            print(f'記者: {news["reporter"]}')
            print(f'內容: {news["content"]}')
            print(f'日期: {news["date"]}')
            print(f'發佈日期: {news["crawl_time"]}')
            print('---')

def main():
    kafka_config = {
        'bootstrap.servers': '104.155.214.8:9092',
        'max.in.flight.requests.per.connection': 1,
    }
    
    scraper = ETTodayNewsScraper(kafka_config=kafka_config, topic='news-topic')
    
    try:
        print("\n獲取首頁新聞:")
        homepage_news = scraper.fetch_homepage_news()
        scraper.print_news(homepage_news)
        
        for category in scraper.news_categories.keys():
            print(f"\n獲取{category}類別的新聞:")
            news_list = scraper.fetch_news(category)
            scraper.print_news(news_list)
    
    finally:
        if scraper.producer:
            scraper.producer.flush()
            print("所有消息已發送完成")

if __name__ == "__main__":
    main()