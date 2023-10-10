from pymongo import MongoClient

def store_to_mongodb(data_list, url, database, collection):
    # 连接MongoDB数据库
    client = MongoClient(url)
    db = client[database]
    collection = db[collection]

    # 将数据项插入MongoDB数据库中
    result = collection.insert_many({'data': data} for data in data_list)

    # 输出插入数据的ID
    for id in result.inserted_ids:
        print(f"插入文档的ID为：{id}")

    # 关闭数据库连接
    client.close()