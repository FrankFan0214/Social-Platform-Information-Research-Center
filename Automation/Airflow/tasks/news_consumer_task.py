from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient, errors
import json
import sys
from utils.kafkaset import error_cb, try_decode_utf8, print_assignment, print_revoke, check_mongodb_connection

def kafka_consumer_job():
    props = {
        'bootstrap.servers': '104.155.214.8:9092',
        'group.id': 'news',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
        'error_cb': error_cb
    }

    consumer = Consumer(props)
    topicName = 'news-topic'
    consumer.subscribe([topicName], on_assign=print_assignment, on_revoke=print_revoke)

    mongo_client = MongoClient("mongodb://airflow:airflow@35.189.181.117:28017/admin")
    check_mongodb_connection(mongo_client)
    db = mongo_client['kafka']
    collection = db['News']

    message_count = 0
    batch_count = 200

    try:
        while True:
            records = consumer.consume(num_messages=500, timeout=1.0)
            if not records:
                continue

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
                    msgKey = try_decode_utf8(record.key())
                    msgValue = try_decode_utf8(record.value())
                    print(f'{topic}-{partition}-{offset}: (key={msgKey}, value={msgValue})')

                    try:
                        json_data = json.loads(msgValue)
                    except json.JSONDecodeError as e:
                        print(f"Failed to decode JSON: {e}")
                        continue

                    document = {
                        'topic': topic,
                        'partition': partition,
                        'offset': offset,
                        'key': msgKey,
                        'value': json_data
                    }
                    collection.update_one({'key':msgKey}, {'$set':document}, upsert=True)
                    # collection.insert_one(document)
                    print(f"Inserted document into MongoDB: {document}")

                    message_count += 1
                    if message_count % batch_count == 0:
                        print(f"已處理 {message_count} 筆資料")

    except KeyboardInterrupt:
        sys.stderr.write('Consumer interrupted by user\n')
    except KafkaException as e:
        sys.stderr.write(f"Kafka exception: {e}\n")
    finally:
        consumer.close()
        mongo_client.close()
