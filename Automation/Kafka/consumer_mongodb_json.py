from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient, errors
import json  # 用來將 JSON 字符串轉換為字典
import sys

# 錯誤處理
def error_cb(err):
    print(f'Error: {err}')

# 解碼 UTF-8 資料
def try_decode_utf8(data):
    return data.decode('utf-8') if data else None

# Partition assignment callback
def print_assignment(consumer, partitions):
    print('Partitions assigned:', [f"{p.topic}-{p.partition}" for p in partitions])

# Partition revoke callback
def print_revoke(consumer, partitions):
    print('Partitions revoked:', [f"{p.topic}-{p.partition}" for p in partitions])

# 檢測 MongoDB 是否連接成功
def check_mongodb_connection(client):
    try:
        client.server_info()  # 測試連接
        print("Successfully connected to MongoDB")
    except errors.ServerSelectionTimeoutError as err:
        print(f"Failed to connect to MongoDB: {err}")
        sys.exit(1)

if __name__ == '__main__':
    # Kafka 消費者配置
    props = {
        'bootstrap.servers': '104.155.214.8:9092',  # Kafka broker 地址
        'group.id': 'json-test',  # Consumer Group ID
        'auto.offset.reset': 'earliest',  # 從最早的 offset 開始消費
        'enable.auto.commit': True,  # 自動提交 offset
        'auto.commit.interval.ms': 5000,  # 每 5 秒自動提交一次
        'error_cb': error_cb  # 錯誤回調函數
    }

    # 創建 Kafka 消費者實例
    consumer = Consumer(props)

    # 訂閱 Kafka topic
    topicName = 'test-topic'
    consumer.subscribe([topicName], on_assign=print_assignment, on_revoke=print_revoke)

    # 連接到 MongoDB
    mongo_client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)  # 替換為你的 MongoDB 連接 URI
    check_mongodb_connection(mongo_client)

    # 獲取資料庫和集合
    db = mongo_client['kafka']  # 替換為你的資料庫名稱
    collection = db['kafka_collection_json_test2']  # 替換為你的集合名稱

    try:
        while True:
            # 批次消費訊息
            records = consumer.consume(num_messages=500, timeout=1.0)
            if not records:
                continue

            for record in records:
                if record.error():
                    if record.error().code() == KafkaError.PARTITION_EOF:
                        # Partition 已到達結尾
                        sys.stderr.write(f'Partition EOF at offset {record.offset()}\n')
                    else:
                        raise KafkaException(record.error())
                else:
                    # 處理訊息
                    topic = record.topic()
                    partition = record.partition()
                    offset = record.offset()
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())
                    print(f'{topic}-{partition}-{offset}: (key={msgKey}, value={msgValue})')

                    # **將 msgValue 轉換為 JSON 字典**
                    try:
                        json_data = json.loads(msgValue)  # 將 JSON 字符串轉換為 Python 字典
                    except json.JSONDecodeError as e:
                        print(f"Failed to decode JSON: {e}")
                        continue  # 跳過無法解析的訊息

                    # 構建 MongoDB 插入的文檔
                    document = {
                        'topic': topic,
                        'partition': partition,
                        'offset': offset,
                        'key': msgKey,
                        'value': json_data  # 存入轉換後的 JSON 字典
                    }

                    # 將資料插入到 MongoDB
                    collection.insert_one(document)
                    print(f"Inserted document into MongoDB: {document}")

    except KeyboardInterrupt:
        sys.stderr.write('Consumer interrupted by user\n')
    except KafkaException as e:
        sys.stderr.write(f"Kafka exception: {e}\n")
    finally:
        # 關閉消費者
        consumer.close()
        # 關閉 MongoDB 連接
        mongo_client.close()
