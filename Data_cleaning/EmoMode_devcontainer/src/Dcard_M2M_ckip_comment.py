import re
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from ckip_transformers.nlp import CkipNerChunker, CkipPosTagger, CkipWordSegmenter
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ServerSelectionTimeoutError

# 初始化 MongoDB 連接
client = MongoClient('mongodb://airflow:airflow@10.140.0.11:28017/admin')
db = client["kafka"]
collection = db["Dcard"]
db2 = client["cleandata"]
output_collection = db2["ckip_data_comment"]

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

# 單則留言處理函數
def process_comment(comment_text, text_date):
    preprocessed_content = preprocess_text(comment_text)
    if not preprocessed_content:
        return None

    try:
        ws_result = ws_driver([preprocessed_content])[0]
        pos_result = pos_driver([ws_result])[0]
        ner_result = ner_driver([preprocessed_content])[0]
        word_frequency = calculate_word_frequency(ws_result)

        word_pos_data = [{"word": word, "pos": pos, "frequency": word_frequency[word]}
                         for word, pos in zip(ws_result, pos_result)]

        ner_data = []
        if isinstance(ner_result, list) and all(isinstance(ner, tuple) for ner in ner_result):
            ner_counter = Counter([ner[0] for ner in ner_result])
            ner_data = [{"entity": ner[0], "type": ner[1], "counts": ner_counter[ner[0]], "publish_date": text_date}
                        for ner in ner_result]
        else:
            ner_counter = Counter([ner["entity"] for ner in ner_result])
            ner_data = [{"entity": ner["entity"], "type": ner["type"], "counts": ner_counter[ner["entity"]], "publish_date": text_date}
                        for ner in ner_result]

    except Exception as e:
        print(f"Error processing comment: {e}")
        return None

    return {
        "content": preprocessed_content,
        "word_pos_frequency": word_pos_data,
        "named_entities": ner_data
    }

# 單篇文章處理函數
def process_item(item):
    value = item["value"]
    
    # 檢查是否已處理過
    if output_collection.find_one({"url": item["key"]}):
        print(f"跳過已處理的文章: {item['key']}")
        return None

    # 日期處理
    try:
        text_date = pd.to_datetime(value.get("發布時間"), errors="coerce")
    except Exception:
        text_date = None

    # 處理留言
    processed_comments = []
    comments = value.get("留言", [])
    for comment in comments:
        for key, comment_data in comment.items():
            comment_text = comment_data.get("內容", "")
            processed_comment = process_comment(comment_text, text_date)
            processed_comments.append({
                "user": comment_data.get("用戶"),
                "time": comment_data.get("時間"),
                "data": processed_comment
            })

    # 構建更新操作，基於 `url` 更新或插入
    processed_data = {
        "url": item["key"],
        "data": {
            "source": 'dcard',
            "publish_date": text_date,
            "title": value.get("標題"),
            "author": value.get("作者"),
            "content": preprocess_text(value.get("內容", "")),
            "emoji_type": value.get("emoji類型", {}),
            "hash_tags": value.get("hash_tag", []),
            "board": value.get("看板", ""),
            "comments": processed_comments
        }
    }

    return UpdateOne({"url": item["key"]}, {"$set": processed_data}, upsert=True)

# 批次處理並釋放記憶體
def process_and_store_data(batch_size=20, fetch_size=100, max_records=None):
    processed_count = 0
    start_time = time.time()

    while True:
        if max_records is not None:
            remaining_records = max_records - processed_count
            if remaining_records <= 0:
                break
            fetch_limit = min(fetch_size, remaining_records)
        else:
            fetch_limit = fetch_size

        data = list(collection.find({}, {"key": 1, "value": 1}).limit(fetch_limit).skip(processed_count))
        if not data:
            break

        operations = []
        for i in range(0, len(data), batch_size):
            batch_data = data[i:i + batch_size]
            with ThreadPoolExecutor(max_workers=4) as executor:
                operations.extend(executor.map(process_item, batch_data))

        if operations:
            output_collection.bulk_write(operations)
            processed_count += len(operations)
            print(f"已完成 {processed_count} 篇文章的處理並存入 MongoDB。")

            if processed_count % 50 == 0:
                elapsed_time = time.time() - start_time
                print(f"已完成 {processed_count} 篇文章的處理，經過時間：{elapsed_time:.2f} 秒")

        del operations, data, batch_data
        import gc
        gc.collect()

    total_time = time.time() - start_time
    print(f"數據處理完成，總運行時間：{total_time:.2f} 秒")

# 執行處理
process_and_store_data(max_records=1000)
print("數據處理完成並存儲至 MongoDB collection: ckip_data_comment")