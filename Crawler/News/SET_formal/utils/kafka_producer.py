from confluent_kafka import Producer

class KafkaProducer:
    def __init__(self, servers, topic):
        """
        初始化 KafkaProducer 類別，設定 Kafka 連接屬性並初始化 Producer 實例。
        
        :param servers: Kafka 伺服器位址（bootstrap.servers）
        :param topic: 要發送訊息的 Kafka topic 名稱
        """
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': servers,
            'max.in.flight.requests.per.connection': 1,
            'error_cb': self.error_callback
        })

    def error_callback(self, err):
        """處理錯誤訊息的回調函數"""
        print(f"Error: {err}")

    def delivery_callback(self, err, msg):
        """處理訊息傳遞結果的回調函數"""
        if err:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def send_message(self, message,key=None):
        """
        發送訊息到 Kafka topic。
        
        :param message: 要發送的訊息
        """
        try:
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=message,
                callback=self.delivery_callback
            )
            self.producer.poll(0)  # 立即處理所有排隊的回調
        except Exception as e:
            print(f"Failed to send message: {e}")

    def flush(self):
        """將所有在隊列中的訊息推送到 Kafka"""
        self.producer.flush()
        
    def close(self):
        self.flush()  # 可以在 close 中再次調用 flush 以確保所有消息已發送
        self.producer = None  # 關閉後設置 producer 為 None