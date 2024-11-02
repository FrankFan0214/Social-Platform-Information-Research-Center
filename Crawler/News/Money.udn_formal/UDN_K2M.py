from utils.kafka_consumer import KafkaConsumerJob

if __name__ == '__main__':
    kafka_servers = '<IP>:9092'
    kafka_group_id = 'news'
    topic_name = 'news-topic'
    mongo_uri = "mongodb://XXXX:XXXX@<IP>:28017/admin"
    mongo_db = 'kafka'
    mongo_collection = 'News_test'

    job = KafkaConsumerJob(
        kafka_servers=kafka_servers,
        kafka_group_id=kafka_group_id,
        topic_name=topic_name,
        mongo_uri=mongo_uri,
        mongo_db=mongo_db,
        mongo_collection=mongo_collection
    )

    job.run()