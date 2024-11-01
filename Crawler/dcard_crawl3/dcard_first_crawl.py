#--------------------爬蟲---------------------------------------
import re
import json
from time import sleep
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
#------------------------MySQL-----------------------------------
import pymysql
connection = pymysql.connect(
    host='34.81.244.193',  # 主機名稱
    port=3306, # 指定 MySQL 使用的端口號
    user='user2',  # 資料庫使用者名稱
    password='password2',  # 資料庫密碼
    database='DCARD',  # 資料庫名稱
    charset='utf8mb4',  # 使用的編碼
    # cursorclass=pymysql.cursors.DictCursor  # 以字典形式返回查詢結果
)
# 建立游標
cursor = connection.cursor()
#---------------kafka-------------------------
from confluent_kafka import Producer
# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)
# 步驟1. 設定要連線到Kafka集群的相關設定
props = {
        # Kafka集群在那裡?
        'bootstrap.servers': '104.155.214.8:9092',  # <-- 置換成要連接的Kafka集群
        'max.in.flight.requests.per.connection': 1,
        'error_cb': error_cb                    # 設定接收error訊息的callback函數
    }
# 步驟2. 產生一個Kafka的Producer的實例
producer = Producer(props)
# 步驟3. 指定想要發佈訊息的topic名稱
topicName = 'dcard-topic'
#-----------------------------爬蟲---------------------------
select_url = """select Url 
                from article_confirm
                where first_crawl = 0
                """ # 如果爬到一半被擋，想要從上次爬到的地方，可以設where條件 first_crawl = 0
cursor.execute(select_url)
result = cursor.fetchall()

for row in result:
    # 取得文章url
    url = row[0]
    # 抓取文章ID
    article_ID = url.split('/')[-1]
    driver1 = uc.Chrome()
    driver1.get('https://www.google.com')
    # 定位google搜尋的位置
    search = driver1.find_element(By.NAME, "q")
    search.send_keys(f"{url}")
    search.send_keys(Keys.ENTER)
    sleep(10)
    # 如果搜尋不到文章，就會跳過
    try:
        # 進入dcard文章      
        driver1.find_element(By.XPATH, f'//*[@id="rso"]/div[1]/div/div/div[1]/div/div/span/a[@href="{url}"]').click()
        sleep(10)
    except Exception:
        #如果文章進不去，把first_crawl=1, confirm_num不改才不會進到第三階段
        current_date = datetime.now().date()
        update_first_crawl = """UPDATE article_confirm
                                SET first_crawl = 1,
                                    last_update = %s
                              WHERE Url = %s"""
        cursor.execute(update_first_crawl, (current_date, url))
        connection.commit()
        driver1.quit()
        continue
    try:
        # 抓取標題
        title = driver1.find_element(By.CLASS_NAME, "t17vlqzd").text
    except Exception:
        #如果文章刪除，把first_crawl=1, confirm_num不改才不會進到第三階段
        current_date = datetime.now().date()
        update_first_crawl = """UPDATE article_confirm
                                SET first_crawl = 1,
                                    last_update = %s
                              WHERE Url = %s"""
        cursor.execute(update_first_crawl, (current_date, url))
        connection.commit()
        driver1.quit()
        continue
    # 抓取看版類型
    type = driver1.find_element(By.CLASS_NAME, f"tcjsomj").text
    # 抓取作者
    author = driver1.find_element(By.CLASS_NAME, f"avvspio").text
    # 抓取發布時間(抓的時間是格林威治標準時間，所以還要再+8才是台灣時間)
    time = driver1.find_element(By.TAG_NAME, "time").get_attribute('datetime')
    # 抓取emoji數
    try:
        emoji_num = driver1.find_element(By.CLASS_NAME, f"s1r6dl9").text
    except NoSuchElementException:
        emoji_num = 0
    except StaleElementReferenceException:
        emoji_num = 0
    # 抓取hash tag
    try:
        hash_tag = []
        element_by_hash_tag = driver1.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div/div/div/div/article/div[3]/div').text
        for word in element_by_hash_tag.split('\n'):
            hash_tag.append(word)
    except NoSuchElementException:
        hash_tag = []
    try:
        # 抓取文章內容
        article_content = ''
        element_by_class = driver1.find_element(By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div/div/div/div/article/div[2]/div/div')
        element_by_span = element_by_class.find_elements(By.TAG_NAME, "span")
        # 將文章內容存成字串
        for span in element_by_span:
            content = re.findall(r'.{1}',span.text)
            for word in content:
                article_content += word
    except NoSuchElementException:
        article_content = ''
    # 進入emoji小頁面
    #----------------------------------------------------------------------------------------
    try:
        # 先移到開啟emoji小頁面的地方
        little_page = driver1.find_element(By.CLASS_NAME, 'r1skb6m4')
        driver1.execute_script("arguments[0].scrollIntoView({block:'center'});", little_page)
        sleep(2)
        driver1.find_element(By.CLASS_NAME, 'r1skb6m4').click()
        sleep(5)
        # 各個emoji數
        emojis = []
        type_emoji = {}
        element_by_emojis = driver1.find_elements(By.CLASS_NAME, 'irn7u4a')
        for each in element_by_emojis:
            key = each.text.split('\n')[0]
            value  = each.text.split('\n')[1]
            type_emoji[key] = value
        emojis.append(type_emoji)
    except NoSuchElementException:
        emojis = []
    try:
        # 離開小頁面
        leave = driver1.find_element(By.CLASS_NAME, "mn4uf0t")
        # 模擬按下 ESC 鍵
        leave.send_keys(Keys.ESCAPE)
    except NoSuchElementException:
        pass
    #-------------------------------------------------------------------------------------
    try:
        # 爬取留言
        messages = []
        each_message = {}
        message_no = {}
        # 移至留言區
        mes_start = driver1.find_element(By.CLASS_NAME, 'd1vdw76m')
        driver1.execute_script("arguments[0].scrollIntoView({block:'center'});", mes_start)
        # 如果找得到新至舊的留言區就使用，沒有就直接爬
        try:
            driver1.find_elements(By.CLASS_NAME, 'oqcw3sj')[2].click()
        except NoSuchElementException:
            pass
        except IndexError:
            pass
        sleep(10)
        # 定位第一個留言
        i = int(driver1.find_element(By.CLASS_NAME, 'c1cbe1w2').get_attribute('data-doorplate'))
        # 如果是新至舊的留言，從最新跑到最舊
        if i > 1:
            while i >= 1:
                while True:
                    try:    
                        # 定位留言區域
                        data_doorplate = driver1.find_element(By.CSS_SELECTOR, f'div[data-doorplate="{i}"]')
                        driver1.execute_script("arguments[0].scrollIntoView({block:'start'});", data_doorplate)
                        break
                    except Exception:
                        # 如果找不到下一個留言，就往下滑直到找到
                        driver1.execute_script("window.scrollBy(0, 500);")
                        sleep(0.5)
                try:
                    message = data_doorplate.find_element(By.CLASS_NAME, f'c19xyhzv')
                    # 抓取樓數
                    mes_no = message.find_element(By.CLASS_NAME, f'dl7cym2').text
                    # 抓取留言者
                    mes_writer = message.find_element(By.CLASS_NAME, f'tygfsru').text
                    # 抓取內容
                    mes_content = message.find_element(By.CLASS_NAME, f'c1ehvwc9').text
                    # 抓取時間(他用的是GMT)
                    mes_time = message.find_element(By.TAG_NAME, 'time').get_attribute('datetime')
                    message_no[mes_no] = {'用戶': mes_writer, '內容': mes_content, '時間': mes_time}
                    messages.append(message_no)
                    message_no = {}
                except Exception:
                    i -= 1
                    continue
                i -= 1
                driver1.execute_script("arguments[0].scrollIntoView({block:'start'});", message)
                sleep(1)
        # 如果是一般的留言
        else:
            move = 0
            while True:
                while True:
                    try:    
                        # 定位留言區域
                        count = 1
                        data_doorplate = driver1.find_element(By.CSS_SELECTOR, f'div[data-doorplate="{i}"]')
                        driver1.execute_script("arguments[0].scrollIntoView({block:'start'});", data_doorplate)
                        break
                    except Exception:
                        # 如果找不到下一個留言，就往下滑直到找到，當滑動十次還找不到，就代表已經滑到最底了，可以結束
                        driver1.execute_script("window.scrollBy(0, 500);")
                        move += count
                        if move == 10:
                            break
                        sleep(0.5)
                if move == 10:
                    break
                try:
                    message = data_doorplate.find_element(By.CLASS_NAME, f'c19xyhzv')
                    mes_no = message.find_element(By.CLASS_NAME, f'dl7cym2').text
                    mes_writer = message.find_element(By.CLASS_NAME, f'tygfsru').text
                    mes_content = message.find_element(By.CLASS_NAME, f'c1ehvwc9').text
                    mes_time = message.find_element(By.TAG_NAME, 'time').get_attribute('datetime')
                    message_no[mes_no] = {'用戶': mes_writer, '內容': mes_content, '時間': mes_time}
                    messages.append(message_no)
                    message_no = {}
                except Exception:
                    i += 1
                    continue
                i += 1
                driver1.execute_script("arguments[0].scrollIntoView({block:'start'});", message)
                sleep(1)
    except Exception:
        pass
    # 離開文章
    sleep(10)
    driver1.quit()
    data1 = {
        "文章ID": article_ID, 
        "作者": author, 
        "標題": title, 
        "連結": url, 
        "發布時間": time, 
        "內容": article_content, 
        "總emoji數": emoji_num, 
        "emoji類型": emojis, 
        "留言":messages, 
        "hash_tag": hash_tag, 
        "看版": type}
    json_data = json.dumps(data1, ensure_ascii=False)
    producer.produce(topicName, key = url, value = json_data.encode('utf-8'))
    producer.flush()
    print(f"已傳送文章至 Kafka: {title}")
        
    current_date = datetime.now().date()
    update_first_crawl = """UPDATE article_confirm
                            SET first_crawl = 1,
                                confirm_num = 1,
                                last_update = %s
                            WHERE Url = %s"""
    cursor.execute(update_first_crawl, (current_date, url))
    connection.commit()
    
cursor.close()
connection.close()  # 關閉連線

    
    

    