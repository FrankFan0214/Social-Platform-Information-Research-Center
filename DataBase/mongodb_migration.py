from pymongo import MongoClient

# 連接到 MongoDB Atlas（雲端）
atlas_uri = "mongodb+srv://TIR103:password6341@tir103.higsi.mongodb.net/?retryWrites=true&w=majority"
atlas_client = MongoClient(atlas_uri,tls=True,tlsCAFile='/etc/ssl/cert.pem')

# 連接到本地 MongoDB
local_uri = "mongodb://localhost:27017/"
local_client = MongoClient(local_uri)

# 指定要遷移的資料庫和集合名稱
atlas_db = atlas_client['kafka']  # 替換為你在 Atlas 上的資料庫名稱
atlas_collection = atlas_db['kafka_collection_json_test2']  # 替換為你要遷移的集合名稱

local_db = local_client['kafka']  # 替換為本地 MongoDB 的資料庫名稱
local_collection = local_db['mongo_migratoin_test']  # 替換為本地 MongoDB 的集合名稱

# 從 MongoDB Atlas 中讀取資料並插入到本地 MongoDB
def migrate_data():
    try:
        # 從 Atlas 讀取所有資料
        documents = atlas_collection.find()

        # 將每個文檔插入到本地 MongoDB
        for doc in documents:
            # 移除 _id 欄位，避免 _id 重複衝突
            doc.pop("_id", None)
            local_collection.insert_one(doc)
        
        print("Data migration completed successfully!")
    
    except Exception as e:
        print(f"An error occurred during migration: {e}")

# 開始遷移
migrate_data()

# 關閉連接
atlas_client.close()
local_client.close()
