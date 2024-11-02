from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient, errors
import json
import sys
from utils.kafkaset import error_cb, try_decode_utf8, print_assignment, print_revoke, check_mongodb_connection

class KafkaConsumerJob:
    def __init__(self, kafka_servers, kafka_group_id, topic_name, mongo_uri, mongo_db, mongo_collection):
        # Kafka 消費者屬性
        self.kafka_props = {
            'bootstrap.servers': kafka_servers,
            'group.id': kafka_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'error_cb': error_cb
        }
        
        # 初始化 Kafka 消費者
        self.consumer = Consumer(self.kafka_props)
        self.topic_name = topic_name
        self.consumer.subscribe([self.topic_name], on_assign=print_assignment, on_revoke=print_revoke)

        # 初始化 MongoDB 客戶端
        self.mongo_client = MongoClient(mongo_uri)
        check_mongodb_connection(self.mongo_client)
        self.db = self.mongo_client[mongo_db]
        self.collection = self.db[mongo_collection]

        # 計數器和批次數
        self.message_count = 0
        self.batch_count = 200

    def process_records(self, records):
        """處理 Kafka 消息並將其插入 MongoDB"""
        for record in records:
            if record.error():
                if record.error().code() == KafkaError.PARTITION_EOF:
                    sys.stderr.write(f'Partition EOF at offset {record.offset()}\n')
                else:
                    raise KafkaException(record.error())
            else:
                topic = record.topic()
                partition = record.partition()
                offset = record.offset()
                msg_key = try_decode_utf8(record.key())
                msg_value = try_decode_utf8(record.value())
                print(f'{topic}-{partition}-{offset}: (key={msg_key}, value={msg_value})')

                # 解析 JSON 數據
                try:
                    json_data = json.loads(msg_value)
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON: {e}")
                    continue

                # 建立 MongoDB 文件
                document = {
                    'topic': topic,
                    'partition': partition,
                    'offset': offset,
                    'key': msg_key,
                    'value': json_data
                }
                self.collection.update_one({'key': msg_key}, {'$set': document}, upsert=True)
                print(f"Inserted document into MongoDB: {document}")

                # 更新計數器
                self.message_count += 1
                if self.message_count % self.batch_count == 0:
                    print(f"已處理 {self.message_count} 筆資料")

    def run(self):
        """開始消費 Kafka 消息並存儲到 MongoDB"""
        try:
            while True:
                records = self.consumer.consume(num_messages=500, timeout=1.0)
                if not records:
                    continue

                self.process_records(records)

        except KeyboardInterrupt:
            sys.stderr.write('Consumer interrupted by user\n')
        except KafkaException as e:
            sys.stderr.write(f"Kafka exception: {e}\n")
        finally:
            self.close()

    def close(self):
        """關閉 Kafka 消費者和 MongoDB 連接"""
        self.consumer.close()
        self.mongo_client.close()
        print("Closed Kafka consumer and MongoDB connection")



