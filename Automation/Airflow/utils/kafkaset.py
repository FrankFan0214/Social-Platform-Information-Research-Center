from confluent_kafka import KafkaException, KafkaError
from pymongo import errors
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
