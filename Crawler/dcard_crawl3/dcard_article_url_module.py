#------------------爬蟲--------------------------------------------
from time import sleep
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
#----------------------MySQL--------------------------------------
import pymysql
connection = pymysql.connect(
    host='<IP>',  # 主機名稱
    port=3306, # 指定 MySQL 使用的端口號
    user='xxxx',  # 資料庫使用者名稱
    password='xxxx',  # 資料庫密碼
    database='DCARD',  # 資料庫名稱
    charset='utf8mb4',  # 使用的編碼
)
# 建立游標
cursor = connection.cursor()
#-------------------------------------------------------------------
def crawl_home_url(head, dcard_url, num_articles):
    # 使用 undetected_chromedriver 初始化 Chrome 瀏覽器 (用來爬標題和連結)
    driver = uc.Chrome()
    # 前往dcard 首頁
    driver.get(dcard_url)
    # 等待頁面載入
    sleep(10)
    for i in range(head, num_articles+head):
        # 預防爬的過程出問題，還是可以把爬到的存下來
        try:
            # if i == block:
            #     continue
            while True:
                try:
                    # 定位連結位址
                    element_by_data_key = driver.find_element(By.XPATH, f"//div[@data-key='{i}']")
                    break
                # 當抓不到data_key就往下滑動一點，直到找到為止       
                except Exception:
                    driver.execute_script("window.scrollBy(0, 500);")
            # 抓取連結
            url = element_by_data_key.find_element(By.CLASS_NAME, "t1gihpsa").get_attribute("href")
            # 抓取文章ID
            article_ID = url.split('/')[-1]
            # 設定爬取日期
            current_date = datetime.now().date()
            # 將data insert 到 MySQL
            insert = """insert ignore into article_confirm(ID, Date, Url)
                            values(%s, %s, %s)"""
            data = (article_ID, current_date, f"{url}")
            cursor.execute(insert, data)
            connection.commit()
            # 頁面向下滾動
            driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
            sleep(1)
        except Exception:
            continue
    driver.quit()
def crawl_url(block, head, dcard_url, num_articles):
    # 使用 undetected_chromedriver 初始化 Chrome 瀏覽器 (用來爬標題和連結)
    driver = uc.Chrome()
    # 前往dcard 首頁
    driver.get(dcard_url)
    # 等待頁面載入
    sleep(10)
    for i in range(head, num_articles+head):
        # 預防爬的過程出問題，還是可以把爬到的存下來
        try:
            if i == block:
                continue
            while True:
                try:
                    # 定位連結位址
                    element_by_data_key = driver.find_element(By.XPATH, f"//div[@data-key='{i}']")
                    break
                # 當抓不到data_key就往下滑動一點，直到找到為止       
                except Exception:
                    driver.execute_script("window.scrollBy(0, 500);")
            # 抓取連結
            url = element_by_data_key.find_element(By.CLASS_NAME, "t1gihpsa").get_attribute("href")
            # 抓取文章ID
            article_ID = url.split('/')[-1]
            # 設定爬取日期
            current_date = datetime.now().date()
            # 將data insert 到 MySQL
            insert = """insert ignore into article_confirm(ID, Date, Url)
                            values(%s, %s, %s)"""
            data = (article_ID, current_date, f"{url}")
            cursor.execute(insert, data)
            connection.commit()
            # 頁面向下滾動
            driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
            sleep(1)
        except Exception:
            continue
    driver.quit()
# dcard 時事版
crawl_url(9, 2, "https://www.dcard.tw/f/trending", 100)
sleep(10)
# dcard 科技版
crawl_url(7, 1, "https://www.dcard.tw/f/tech_job", 100)
sleep(10)
# dcard首頁
crawl_home_url(0, "https://www.dcard.tw/f", 100)
cursor.close()
connection.close()  # 關閉連線