import pandas as pd
import glob
from pymongo import MongoClient

mongo_host = 'mongodb://readwrite:readwrite@59.73.198.168/?authMechanism=DEFAULT'
mongo_port = 27017
database_name = 'PharmRG'
collection_name = 'FAERS'



# 连接 MongoDB
client = MongoClient(mongo_host, mongo_port)
db = client[database_name]
collection = db[collection_name]

print("连接成功")

# 获取所有文件的路径
# file_paths = glob.glob("your_directory/*.csv")  # 替换为你存储FAERS数据库文件的文件夹路径
file_paths = glob.glob("DEMO18Q2.csv") + glob.glob("DRUG18Q2.csv")+glob.glob("INDI18Q2.csv")+glob.glob("OUTC18Q2.csv")+glob.glob("REAC18Q2.csv")+glob.glob("RPSR18Q2.csv")+glob.glob("THER18Q2.csv")

# 创建一个空的DataFrame来存储所有数据
all_data = pd.DataFrame()

# 逐个读取文件并合并数据
for file_path in file_paths:
    data = pd.read_csv(file_path, low_memory=False)  # 读取文件
    all_data = pd.concat([all_data, data], ignore_index=True)  # 连接数据

# 根据primaryid合并数据
merged_data = all_data.groupby('primaryid').agg(lambda x: x.iloc[0])


merged_data.fillna('', inplace=True)


# 将合并后的数据转换为字典格式
merged_data_dict = merged_data.to_dict(orient='index')

# 将数据上传到MongoDB
for primaryid, data_dict in merged_data_dict.items():
    collection.update_one({'primaryid': primaryid}, {'$set': data_dict}, upsert=True)

print("数据上传完成！")
