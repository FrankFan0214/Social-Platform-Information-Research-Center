import json
import re
import pandas as pd
from collections import Counter
from datetime import datetime, timezone
from ckip_transformers.nlp import CkipNerChunker, CkipPosTagger, CkipWordSegmenter
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient, UpdateOne
from transformers import AutoTokenizer

# 初始化 CKIP Transformers 的 NER 模型
print("Loading CKIP model...")
tokenizer = AutoTokenizer.from_pretrained("ckiplab/bert-base-chinese")
model = CkipNerChunker(model_name="ckiplab/bert-base-chinese-ner")
ws_driver = CkipWordSegmenter(model="bert-base")
pos_driver = CkipPosTagger(model="bert-base")
ner_driver = CkipNerChunker(model="bert-base")
print("CKIP model loaded")

# 配置 Kafka Consumer
conf = {
    'bootstrap.servers': '<IP>:9092',
    'group.id': '3',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
print("Kafka consumer configured")


def check_mongodb_connection(client):
    """檢查 MongoDB 連接狀態"""
    try:
        client.admin.command('ping')
        print("Successfully connected to MongoDB!")
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        raise


def preprocess_text(text):
    """文本預處理函數"""
    text = re.sub(r"\n+", "\n", text)
    text = re.sub(r"-----\nSent from.*", "", text)
    text = re.sub(r"--+", "", text)
    return text.strip()


def calculate_word_frequency(text):
    """計算文本詞頻"""
    words = ws_driver([text])[0]  # 使用斷詞功能
    word_counts = Counter(words)  # 計算詞頻
    return word_counts


def process_kafka_messages(consumer, collection, message_limit=200): # 設置筆數限制
    """消費 Kafka 消息並處理每條消息"""
    message_count = 0

    # 消費和處理消息
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        # 提取消息的 key 和內容
        msg_key = msg.key().decode('utf-8') if msg.key() else None
        text = msg.value().decode('utf-8')
        print(f"\nReceived message: {text}, Key: {msg_key}")

        try:
            # 解析 JSON 格式的消息
            news_data = json.loads(text)
            preprocessed_text = preprocess_text(news_data.get('content', ''))
            ner_results = model([preprocessed_text])
            
            # 提取 key 欄位
            url = news_data.get('url', None)
            
            # 提取各欄位
            publish_date = news_data.get('date', None)
            title = news_data.get('title', None)
            author = news_data.get('reporter', None)
            
            # 取得命名實體和詞頻資料
            entities = [{"word": entity.word, "type": entity.ner} for entity in ner_results[0]]
            word_frequency = calculate_word_frequency(preprocessed_text)
            

            # 將資料插入到 MongoDB
            document = {
                "data": {
                    'source1': 'news',
                    'publish_date': publish_date,
                    'title': title,
                    'author': author,
                    'content': preprocessed_text,
                    'entities': entities,
                    'word_frequency': dict(word_frequency)
                }
            }

            # 使用 msg_key 作為更新條件
            collection.update_one({'key': msg_key}, {'$set': document}, upsert=True)
            print(f"Document upserted with key: {msg_key}")

            message_count += 1
            print(f"Processed {message_count} messages")

        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
        except Exception as e:
            print(f"Error processing message: {e}")

    print("Message limit reached")


def main():
    try:
        mongo_client = MongoClient("mongodb://xxxx:xxxxx@<IP>:28017/admin")
        check_mongodb_connection(mongo_client)

        collection = mongo_client['cleandata']['CT']

        consumer.subscribe(['news-topic'])
        print("Subscribed to news-topic")

        process_kafka_messages(consumer, collection)

    except KeyboardInterrupt:
        print("\nManually interrupted")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        print("\nCleaning up resources...")
        consumer.close()
        mongo_client.close()
        print("Program finished")


if __name__ == "__main__":
    main()
