import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple

class ETTodayNewsScraper:
    def __init__(self):
        self.base_url = "https://www.ettoday.net"
        self.news_categories = {
            "hot": {
                "url": "https://www.ettoday.net/news/hot-news.htm",
                "selector": "body > div.wrapper_box > div > div.container_box > div > div > div.c1 > div > div.block_content > div > div:nth-child({}) > h3 > a",
                "max_items": 50
            },
            "life": {
                "url": "https://www.ettoday.net/news/focus/%E7%94%9F%E6%B4%BB/",
                "selector": "#lifestyle > div.wrapper_box > div > div.container_box > div > div > div.c1 > div.block.block_1.infinite_scroll > div.block_content > div > div:nth-child({}) > h3 > a",
                "max_items": 20
            },
            # ... other categories omitted for brevity ...
        }

    def _fetch_page_content(self, url: str) -> Optional[BeautifulSoup]:
        """獲取頁面內容並返回BeautifulSoup對象"""
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            print(f"無法連接到頁面，狀態碼: {response.status_code}")
            return None
        except Exception as e:
            print(f"發生錯誤: {str(e)}")
            return None

    def _get_article_content_and_likes_and_date(self, url: str) -> Tuple[Optional[str], int, Optional[str]]:
        """獲取文章內容、按讚數和日期"""
        soup = self._fetch_page_content(url)
        content = None
        likes = 0
        date = None
        
        if soup:
            # 獲取文章內容
            article_content = soup.find('div', class_='story')
            content = article_content.get_text(strip=True) if article_content else None
            
            # 獲取按讚數
            try:
                like_element = soup.find('span', id=lambda x: x and x.startswith('u_0_1_'))
                if like_element:
                    likes_text = like_element.get_text(strip=True)
                    likes = int(''.join(filter(str.isdigit, likes_text))) if likes_text else 0
            except Exception as e:
                print(f"獲取按讚數時發生錯誤: {str(e)}")

            # 獲取日期
            date_element = soup.find('time', class_='date')
            date = date_element.get_text(strip=True) if date_element else None
                
        return content, likes, date
    
    def fetch_homepage_news(self) -> List[Dict]:
        """獲取首頁新聞"""
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

                content, likes, date = self._get_article_content_and_likes_and_date(article_url)
                if content:
                    news_list.append({
                        'title': title_text,
                        'url': article_url,
                        'content': content,
                        'likes': likes,
                        'date': date,
                        'category': 'homepage'
                    })  
        return news_list

    def fetch_news(self, category: str) -> List[Dict]:
        """獲取指定類別的新聞"""
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
                
                content, likes, date = self._get_article_content_and_likes_and_date(article_url)
                if content:
                    news_list.append({
                        'title': title_text,
                        'url': article_url,
                        'content': content,
                        'likes': likes,
                        'date': date,
                        'category': category
                    })
            else:
                print(f"無法找到符合的選擇器: {selector}")

        return news_list

    def print_news(self, news_list: List[Dict]) -> None:
        """打印新聞列表"""
        for idx, news in enumerate(news_list, 1):
            print(f'標題{idx}: {news["title"]}')
            print(f'日期: {news["date"]}')
            print(f'網址: {news["url"]}')
            print(f'內容: {news["content"]}')
            print(f'按讚數: {news["likes"]}')
            print('---')

def main():
    scraper = ETTodayNewsScraper()
    
    # 獲取首頁新聞
    print("\n獲取首頁新聞:")
    homepage_news = scraper.fetch_homepage_news()
    scraper.print_news(homepage_news)
    
    # 獲取所有類別的新聞
    for category in scraper.news_categories.keys():
        print(f"\n獲取{category}類別的新聞:")
        news_list = scraper.fetch_news(category)
        scraper.print_news(news_list)
    

if __name__ == "__main__":
    main()
