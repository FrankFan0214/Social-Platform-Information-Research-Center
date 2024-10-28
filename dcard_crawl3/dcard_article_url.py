#------------------爬蟲--------------------------------------------
from time import sleep
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
#----------------------MySQL--------------------------------------
import pymysql
connection = pymysql.connect(
    host='34.81.244.193',  # 主機名稱
    port=3306, # 指定 MySQL 使用的端口號
    user='user2',  # 資料庫使用者名稱
    password='password2',  # 資料庫密碼
    database='DCARD',  # 資料庫名稱
    charset='utf8mb4',  # 使用的編碼
)
# 建立游標
cursor = connection.cursor()
#-------------------------------------------------------------------
# 使用 undetected_chromedriver 初始化 Chrome 瀏覽器 (用來爬標題和連結)
driver = uc.Chrome()
# 前往dcard 首頁
dcard_url = "https://www.dcard.tw/f" 
driver.get(dcard_url)
# 等待頁面載入
sleep(10)
# 一次爬幾個文章
num_articles = 100
# 中間會有阻擋的，要跳過(每個版不同)
block = 0
# 每個版的頭不同，data-key不同
head = 0
# 初始序號
initial_serial = 1
# 定義ID格式
def generate_code(date,serial):
    # 將月份和日期與序號結合
    return f"{date}{serial:03}"  # 序號以三位數格式填充

for i in range(head, num_articles+head):
    # 預防爬的過程出問題，還是可以把爬到的存下來
    try:
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
        # 設定爬取日期
        current_date = datetime.now().date()
        # ID的前半部分
        date_str = current_date.strftime('%m%d')
        # 將data insert 到 MySQL
        insert = """insert into article_confirm(ID, Date, Url)
                        values(%s, %s, %s)"""
        data = (generate_code(date_str,initial_serial+i), current_date, f"{url}")
        cursor.execute(insert, data)
        connection.commit()
        # 頁面向下滾動
        driver.execute_script("arguments[0].scrollIntoView({block:'start'});", element_by_data_key)
        sleep(1)
    except Exception:
        continue
driver.quit()
cursor.close()
connection.close()  # 關閉連線