from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# MongoDB 連接字符串
url = "mongodb+srv://TIR103:password6341@tir103.higsi.mongodb.net/?retryWrites=true&w=majority"

try:
    # 連接到 MongoDB Atlas，並指定 CA 憑證文件
    mongo_client = MongoClient(url, tls=True, tlsCAFile='/etc/ssl/cert.pem')

    # 測試連接
    mongo_client.admin.command('ping')
    print("Successfully connected to MongoDB Atlas!")

    # 獲取資料庫
    db = mongo_client['kafka']

    # 獲取集合
    collection = db['test_collection']

    # 插入測試文檔
    test_document = {"name": "Test", "value": 123}
    result = collection.insert_one(test_document)
    print(f"Test document inserted with ID: {result.inserted_id}")

    # 查詢測試文檔
    found_document = collection.find_one({"name": "Test"})
    print(f"Found document: {found_document}")

except ConnectionFailure:
    print("Failed to connect to MongoDB Atlas.")
