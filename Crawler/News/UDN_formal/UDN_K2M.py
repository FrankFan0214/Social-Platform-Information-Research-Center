from utils.kafka_consumer import KafkaConsumerJob

if __name__ == '__main__':
    kafka_servers = '<IP>:9092'
    kafka_group_id = 'news'
    topic_name = 'news-topic'
    mongo_uri = "mongodb://xxxx:xxxx@<IP>:28017/admin"
    mongo_db = '<DB_NAME>'
    mongo_collection = '<COLLECTION_NAME>'

    job = KafkaConsumerJob(
        kafka_servers=kafka_servers,
        kafka_group_id=kafka_group_id,
        topic_name=topic_name,
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
        mongo_collection=mongo_collection
    )

    job.run()