import gc
import re
import time
from collections import Counter
from datetime import datetime

import pandas as pd
from ckip_transformers.nlp import CkipNerChunker, CkipPosTagger, CkipWordSegmenter
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ServerSelectionTimeoutError

# 初始化 MongoDB 連接
client = MongoClient('mongodb://xxxx:xxxx@<IP>:28017/admin')
db = client["kafka"]
collection = db["Ptt"]
db_output = client["cleandata"]
content_collection = db_output["ckip_data_content_test"]
comment_collection = db_output["ckip_data_comment_test"]

# 測試連接是否成功
try:
    client.server_info()
    print("MongoDB 連接成功")
except ServerSelectionTimeoutError as err:
    print("MongoDB 連接失敗:", err)

# 初始化 CKIP Transformers
ws_driver = CkipWordSegmenter(model="bert-base")
pos_driver = CkipPosTagger(model="bert-base")
ner_driver = CkipNerChunker(model="bert-base")

# 文本預處理函數
def preprocess_text(text):
    text = re.sub(r"\n+", "\n", text)
    text = re.sub(r"-----\nSent from.*", "", text)
    text = re.sub(r"--+", "", text)
    return text.strip()

# 計算詞頻
def calculate_word_frequency(word_list):
    return Counter(word_list)



# 日期處理函數
def process_date(text_date_str):
    # 預設年份
    default_year = "2024"
    
    try:
        # 如果日期包含"2023"，替換成"2024"
        if "2023" in text_date_str:
            text_date_str = text_date_str.replace("2023", "2024")
        
        # 嘗試解析完整格式 YYYY/MM/DD
        if len(text_date_str.split('/')) == 3:
            text_date = pd.to_datetime(text_date_str, format="%Y/%m/%d").strftime("%Y-%m-%d")
        # 嘗試解析短格式 MM/DD，假設年份為 2024
        elif len(text_date_str.split('/')) == 2:
            text_date = pd.to_datetime(f"{default_year}/{text_date_str}", format="%Y/%m/%d").strftime("%Y-%m-%d")
        else:
            raise ValueError("Unsupported date format")
    except ValueError:
        text_date = None  # 如果無法解析，設置為 None

    return text_date


# 處理單篇文章內容的函數
def process_content_item(item):
    value = item["value"]
    print(f"正在處理文章: {item['key']}")

    # 日期處理
    text_date_str = value.get("date", "未知日期")
    text_date = process_date(text_date_str)
    # 內容預處理
    raw_content = value.get("post", "")
    preprocessed_content = preprocess_text(raw_content)

    # 如果內容是空的，則跳過該文章
    if not preprocessed_content:
        print(f"文章內容為空，跳過: {item['key']}")
        return None
    
    # 分詞、詞性標註、命名實體識別
    ws_result = ws_driver([preprocessed_content])[0]
    pos_result = pos_driver([ws_result])[0]
    ner_result = ner_driver([preprocessed_content])[0]

    # 詞頻計算
    word_frequency = calculate_word_frequency(ws_result)
    word_pos_data = [{"word": word, "pos": pos, "frequency": word_frequency[word]}
                     for word, pos in zip(ws_result, pos_result)]

    # 處理命名實體識別
    ner_counter = Counter([ner[0] for ner in ner_result])
    ner_data = [{"entity": ner[0], "type": ner[1], "counts": ner_counter[ner[0]], "publish_date": text_date}
                for ner in ner_result]

    # 構建更新操作
    processed_data = {
        "url": item["key"],
        "data": {
            "source": 'ptt',
            "publish_date": text_date,
            "title": value.get("title"),
            "author": value.get("author"),
            "content": preprocessed_content,
            "word_pos_frequency": word_pos_data,
            "named_entities": ner_data
        }
    }

    return UpdateOne({"url": item["key"]}, {"$set": processed_data}, upsert=True),text_date

# 處理單則留言的函數
def process_comment(comment, article_key, article_data):
    original_time = comment.get("time", "")
    ip_address = None
    formatted_date = article_data["publish_date"]

    if original_time:
        ip_match = re.match(r"(\d+\.\d+\.\d+\.\d+) (.+)", original_time)
        if ip_match:
            ip_address = ip_match.group(1)
            date_str = ip_match.group(2)
            try:
                formatted_date = pd.to_datetime(f"2024/{date_str}", format="%Y/%m/%d").strftime("%Y-%m-%d")
            except ValueError:
                formatted_date = date_str

    preprocessed_content = preprocess_text(comment.get("comment", ""))
    if not preprocessed_content:
        return None

    ws_result = ws_driver([preprocessed_content])[0]
    pos_result = pos_driver([ws_result])[0]
    ner_result = ner_driver([preprocessed_content])[0]

    word_frequency = calculate_word_frequency(ws_result)
    word_pos_data = [{"word": word, "pos": pos, "frequency": word_frequency[word]}
                     for word, pos in zip(ws_result, pos_result)]

    ner_counter = Counter([ner[0] for ner in ner_result])
    ner_data = [{"entity": ner[0], "type": ner[1], "counts": ner_counter[ner[0]], "publish_date": formatted_date}
                for ner in ner_result]

    return {
        "push": comment.get("push"),
        "user": comment.get("user"),
        "comment": preprocessed_content,
        "ip": ip_address,
        "date": formatted_date,
        "word_pos_frequency": word_pos_data,
        "named_entities": ner_data
    }

# 批次處理並儲存數據
def process_and_store_data(batch_size=20, fetch_size=100, max_records=None):
    processed_count = 0
    start_time = time.time()
    processed_urls = set(content_collection.distinct("url"))

    while True:
        if max_records is not None:
            remaining_records = max_records - processed_count
            if remaining_records <= 0:
                break
            fetch_limit = min(fetch_size, remaining_records)
        else:
            fetch_limit = fetch_size

        data = []
        for _ in range(fetch_limit):
            item = collection.find_one_and_update(
                {"comment_processed": {"$ne": True}, "comment_processing": {"$ne": True}, "key": {"$nin": list(processed_urls)}},
                {"$set": {"comment_processing": True}},
                return_document=True
            )
            if item:
                data.append(item)

        if not data:
            print("無更多未處理的文章。")
            break

        content_operations = []
        comment_documents = []
        update_processed = []

        for item in data:
            text_date = None
            # 文章主體處理並準備插入到 content_collection
            processed_content,text_date = process_content_item(item)
            if processed_content is not None:
                content_operations.append(processed_content)

            # 準備留言格式
            article_data = {
                "source": 'ptt',
                "publish_date": text_date,
                "title": item["value"].get("title", ""),
                "author": item["value"].get("author", ""),
                "content": preprocess_text(item["value"].get("post", ""))
            }

            # 處理所有留言並準備插入到 comment_collection
            article_key = item["key"]
            processed_comments = [process_comment(comment, article_key, article_data) for comment in item["value"].get("comment_num", []) if process_comment(comment, article_key, article_data) is not None]

            if processed_comments:
                comment_document = {
                    "url": article_key,
                    "data": article_data,
                    "comments": processed_comments
                }
                comment_documents.append(comment_document)

            update_processed.append(UpdateOne({"_id": item["_id"]}, {"$set": {"comment_processed": True}, "$unset": {"comment_processing": ""}}))

        if content_operations:
            content_collection.bulk_write(content_operations)
        if comment_documents:
            comment_collection.insert_many(comment_documents)  # 留言集合插入
        if update_processed:
            collection.bulk_write(update_processed)

        processed_count += len(content_operations)
        print(f"已完成 {processed_count} 篇文章的處理並存入 MongoDB。")

        if processed_count % 50 == 0:
            elapsed_time = time.time() - start_time
            print(f"已完成 {processed_count} 篇文章的處理，經過時間：{elapsed_time:.2f} 秒")

        del content_operations, comment_documents, data, update_processed
        gc.collect()

    total_time = time.time() - start_time
    print(f"數據處理完成，總運行時間：{total_time:.2f} 秒")

# 執行處理
process_and_store_data(max_records=1)
print("數據處理完成並存儲至 MongoDB collections: ckip_data_content 和 ckip_data_comment")