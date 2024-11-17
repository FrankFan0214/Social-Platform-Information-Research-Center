import gc
import re
import time
from collections import Counter

import pandas as pd
from ckip_transformers.nlp import CkipNerChunker, CkipPosTagger, CkipWordSegmenter
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ServerSelectionTimeoutError

# 初始化 MongoDB 連接
client = MongoClient("<IP>")
db = client["kafka"]
collection = db["ckip_dcard"]
db2 = client["cleandata"]
comment_collection = db2["ckip_data_comment_dcard"]
content_collection = db2["ckip_data_content_dcard"]

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

# 單篇文章處理函數，僅對 `內容` 欄位進行 CKIP 分析
def process_content_item(item):
    value = item["value"]

    # 顯示正在處理的文章 key
    print(f"正在處理文章: {item['key']}")

    # 日期處理
    try:
        text_date = pd.to_datetime(value.get("date", ""), format="%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
    except ValueError:
        text_date = None

    # 內容預處理和 CKIP 分析
    raw_content = value.get("post", "")
    preprocessed_content = preprocess_text(raw_content)

    # 檢查是否為空
    if not preprocessed_content:
        print("跳過空的內容。")
        return None

    # CKIP 分析
    ws_result = ws_driver([preprocessed_content])[0]
    pos_result = pos_driver([ws_result])[0]
    ner_result = ner_driver([preprocessed_content])[0]

    # 詞頻計算
    word_frequency = calculate_word_frequency(ws_result)
    word_pos_data = [{"word": word, "pos": pos, "frequency": word_frequency[word]}
                     for word, pos in zip(ws_result, pos_result)]

    # 處理命名實體識別結果
    ner_counter = Counter([ner[0] for ner in ner_result])
    ner_data = [{"entity": ner[0], "type": ner[1], "counts": ner_counter[ner[0]], "publish_date": text_date}
                for ner in ner_result]

    # 構建更新操作
    processed_data = {
        "url": item["key"],
        "data": {
            "source": 'dcard',
            "publish_date": text_date,
            "title": value.get("title"),
            "author": value.get("author"),
            "content": preprocessed_content,
            "word_pos_frequency": word_pos_data,
            "named_entities": ner_data,
            "total_emoji_count": value.get("emoji_num", 0),
            "emoji_types": value.get("emoji_type", []),
            "board": value.get("type")
        }
    }

    return UpdateOne({"url": item["key"]}, {"$set": processed_data}, upsert=True)

# 單篇文章處理函數，僅對 `留言` 欄位進行 CKIP 分析
def process_comments_item(item):
    value = item.get("value", {})
    if not value:
        print(f"未找到文章值，跳過: {item}")
        return None

    # 顯示正在處理的文章 key
    print(f"正在處理文章: {item.get('key')}")

    # 日期處理
    try:
        text_date = pd.to_datetime(value.get("date", ""), format="%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        text_date = None

    # 取得文章內容
    raw_content = value.get("post", "")
    # 處理留言
    comments = value.get("comment_num", [])
    processed_comments = []
    for comment in comments:
        for key, details in comment.items():
            raw_comment_content = details.get("comment", "")
            preprocessed_comment = preprocess_text(raw_comment_content)

            # 檢查是否為空
            if not preprocessed_comment:
                print(f"跳過空的留言: {key}")
                continue

            # CKIP 分析
            ws_result = ws_driver([preprocessed_comment])[0]
            pos_result = pos_driver([ws_result])[0]
            ner_result = ner_driver([preprocessed_comment])[0]

            # 詞頻計算
            word_frequency = calculate_word_frequency(ws_result)
            word_pos_data = [{"word": word, "pos": pos, "frequency": word_frequency[word]}
                             for word, pos in zip(ws_result, pos_result)]

            # 處理命名實體識別結果
            ner_counter = Counter([ner[0] for ner in ner_result])
            ner_data = [{"entity": ner[0], "type": ner[1], "counts": ner_counter[ner[0]], "publish_date": text_date}
                        for ner in ner_result]

            processed_comments.append({
                "comment_id": key,
                "user": details.get("user"),
                "content": preprocessed_comment,
                "word_pos_frequency": word_pos_data,
                "named_entities": ner_data,
                "publish_date": details.get("time", text_date)
            })

    # 構建更新操作
    processed_data = {
        "url": item.get("key"),
        "data": {
            "source": 'dcard',
            "publish_date": text_date,
            "title": value.get("title"),
            "author": value.get("author"),
            "content": raw_content,
            "comments": processed_comments,
            "total_emoji_count": value.get("emoji_num", 0),
            "emoji_types": value.get("emoji_type", []),
            "board": value.get("type")
        }
    }

    return UpdateOne({"url": item.get("key")}, {"$set": processed_data}, upsert=True)

# 批次處理並存儲數據
def process_and_store_data(batch_size=20, fetch_size=100, max_records=None):
    processed_count = 0
    start_time = time.time()

    # 先取得所有已處理過的文章URL
    processed_urls = set(content_collection.distinct("url"))

    while True:
        if max_records is not None:
            remaining_records = max_records - processed_count
            if remaining_records <= 0:
                break
            fetch_limit = min(fetch_size, remaining_records)
        else:
            fetch_limit = fetch_size

        # 使用 find_one_and_update 查找未在目標 collection 中且未處理過的文檔，設置臨時標記 content_processing
        data = []
        for _ in range(fetch_limit):
            item = collection.find_one_and_update(
                {"content_processed": {"$ne": True}, "content_processing": {"$ne": True}, "key": {"$nin": list(processed_urls)}},
                {"$set": {"content_processing": True}},
                return_document=True
            )
            if item:
                data.append(item)

        if not data:
            print("無更多未處理的文章。")
            break

        operations_content = []
        operations_comment = []
        update_processed = []

        for item in data:
            # 處理文章內容
            processed_content = process_content_item(item)
            if processed_content is not None:
                operations_content.append(processed_content)
            
            # 處理文章留言
            processed_comment = process_comments_item(item)
            if processed_comment is not None:
                operations_comment.append(processed_comment)

            update_processed.append(UpdateOne({"_id": item["_id"]}, {"$set": {"content_processed": True}, "$unset": {"content_processing": ""}}))

        # 打印批量操作的大小，確認每次批量處理的數量
        if operations_content:
            # 執行批量插入和更新
            content_collection.bulk_write(operations_content)
        if operations_comment:
            # 執行批量插入和更新
            comment_collection.bulk_write(operations_comment)
        if update_processed:
            collection.bulk_write(update_processed)

        processed_count += len(operations_content)
        print(f"已完成 {processed_count} 篇文章的處理並存入 MongoDB。")

        if processed_count % 50 == 0:
            elapsed_time = time.time() - start_time
            print(f"已完成 {processed_count} 篇文章的處理，經過時間：{elapsed_time:.2f} 秒")

        # 釋放內存資源
        del operations_content, operations_comment, data, update_processed
        gc.collect()

    total_time = time.time() - start_time
    print(f"數據處理完成，總運行時間：{total_time:.2f} 秒")


process_and_store_data(max_records=None)
print("數據處理完成並存儲至 MongoDB collection: ckip_data_content_dcard-test和ckip_data_comment_dcard-test")