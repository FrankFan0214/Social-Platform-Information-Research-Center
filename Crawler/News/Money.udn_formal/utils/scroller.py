# utils/scroller.py
from time import sleep

class PageScroller:
    def __init__(self, driver, url, max_scrolls=30, scroll_pause=2):
        """
        初始化 PageScroller 類別.

        :param driver: Selenium WebDriver instance
        :param url: Target webpage URL to scroll
        :param max_scrolls: Maximum number of scrolls to perform (default: 30)
        :param scroll_pause: Time to wait after each scroll (in seconds, default: 2)
        """
        self.driver = driver
        self.url = url
        self.max_scrolls = max_scrolls
        self.scroll_pause = scroll_pause
        self.scroll_count = 0

    def open_page(self):
        """ 打開目標網頁 """
        self.driver.get(self.url)
        print(f"已打開頁面: {self.url}")

    def scroll(self):
        """ 滾動到頁面底部，直到達到設定的滾動次數 """
        while self.scroll_count < self.max_scrolls:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            sleep(self.scroll_pause)
            self.scroll_count += 1
            print(f"已滾動次數: {self.scroll_count}")

        print("已達到設定的滾動次數，停止滾動")