import requests
from bs4 import BeautifulSoup
import time
from pymongo import MongoClient

# 連接 MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ptt_2']  # 選擇名為 'ptt_2' 的資料庫
collection = db['posts']  # 選擇名為 'posts' 的集合

base_url = "https://www.ptt.cc"  # PTT 主要網站網址
board_url = "/bbs/Gossiping/index.html"  # 八卦板首頁的 URL
cookies = {'over18': '1'}  # PTT 問卷的 over18 餅乾，設為已滿18歲

max_pages = 10  # 設定要爬取的最大頁數
current_page = 0  # 目前爬取的頁面，初始為0

# 定義函數用來抓取文章內容
def get_post_content(link):
    try:
        # 取得貼文內容
        response = requests.get(link, cookies=cookies)
        response.raise_for_status()  # 確保回應成功
        soup = BeautifulSoup(response.text, 'html.parser')

        # 提取文章主體及作者
        content = soup.find('div', id='main-content')
        meta_info = soup.find_all('span', class_='article-meta-value')
        author = meta_info[0].text.strip() if len(meta_info) > 0 else "未知"

        # 處理推文區域
        pushes = soup.find_all('div', class_='push')
        push_count, boo_count, arrow_count = 0, 0, 0  # 計算推、噓和箭頭數量
        comments = []  # 存放所有留言

        for push in pushes:
            # 提取每條推文的相關資訊
            push_tag = push.find('span', class_='push-tag').text.strip()
            push_user = push.find('span', class_='push-userid').text.strip()
            push_content = push.find('span', class_='push-content').text.strip()
            push_time = push.find('span', class_='push-ipdatetime').text.strip()

            # 根據推文標籤，統計推、噓、箭頭數量
            if push_tag == '推':
                push_count += 1
            elif push_tag == '噓':
                boo_count += 1
            else:
                arrow_count += 1
            
            # 儲存每條留言的相關內容
            comments.append({
                "推文": push_tag,
                "用戶": push_user,
                "內容": push_content[2:],  # 移除 ": " 前綴
                "時間": push_time
            })

        # 如果找到文章內容，去除多餘標籤並返回結果
        if content:
            for tag in content.find_all(['span', 'div']):
                tag.extract()  # 去除文章中無關的 span 和 div
            return {
                "作者": author,
                "內容": content.text.strip(),  # 完整文章內容
                "推": push_count,  # 推文數
                "噓": boo_count,  # 噓文數
                "箭頭": arrow_count,  # 箭頭數
                "留言": comments  # 所有留言
            }
    except requests.RequestException as e:
        # 處理請求錯誤
        print(f"抓取文章內容時發生錯誤: {e}")
    return None  # 若出現錯誤，返回 None

# 開始進行頁面爬取，當前頁面數小於最大頁面數時繼續爬取
while current_page < max_pages:
    try:
        # 抓取當前頁面內容
        response = requests.get(base_url + board_url, cookies=cookies)
        response.raise_for_status()  # 確保頁面回應正常
        soup = BeautifulSoup(response.text, 'html.parser')
        posts = soup.find_all('div', class_='r-ent')  # 尋找所有貼文的區塊

        for post in posts:
            a_tag = post.find('a')  # 找到每篇文章的連結
            if a_tag:
                # 提取文章標題及連結
                title = a_tag.text.strip()
                link = base_url + a_tag['href']  # 使用貼文的完整網址

                # 取得該貼文的詳細內容
                post_data = get_post_content(link)
                date = post.find('div', class_='date').text.strip()  # 提取發佈日期

                if post_data:
                    # 檢查文章是否已存在於資料庫（根據連結）
                    existing_post = collection.find_one({"連結": link})
                    
                    if existing_post:
                        # 如果文章已存在，檢查是否有新留言
                        existing_comments = existing_post.get('留言', [])
                        new_comments = [c for c in post_data['留言'] if c not in existing_comments]

                        if new_comments:
                            # 更新留言字段，僅更新新留言部分
                            collection.update_one(
                                {"連結": link},  # 根據連結找到對應文章
                                {"$set": {
                                    "留言": existing_comments + new_comments  # 新增新留言
                                }}
                            )
                            print(f"文章 {title} 有新留言，已更新。")
                        else:
                            print(f"文章 {title} 沒有新留言。")
                    else:
                        # 如果文章不存在，插入新文章及留言至資料庫
                        post_document = {
                            "發佈日期": date,
                            "標題": title,
                            "作者": post_data["作者"],
                            "內容": post_data["內容"],
                            "推": post_data["推"],
                            "噓": post_data["噓"],
                            "箭頭": post_data["箭頭"],
                            "連結": link,  # 使用貼文的網址作為唯一標識
                            "留言": post_data["留言"]
                        }
                        collection.insert_one(post_document)  # 將新文章插入資料庫
                        print(f"已插入新文章：{title}")

        # 取得上一頁的連結以繼續爬取
        btn_group = soup.find('div', class_='btn-group btn-group-paging')
        prev_link = btn_group.find_all('a')[1]['href']  # 取得 "上一頁" 的連結
        board_url = prev_link  # 將下一個頁面的 URL 設為當前的 URL
        current_page += 1  # 頁數遞增
        time.sleep(5)  # 暫停 2 秒，避免伺服器負荷過大
    except requests.RequestException as e:
        # 處理頁面請求錯誤
        print(f"抓取頁面時發生錯誤: {e}")
        break  # 若出錯，停止爬取
