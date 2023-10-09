import pandas as pd
from pymongo import MongoClient
import re

# 连接MongoDB数据库
client = MongoClient('mongodb://readwrite:readwrite@59.73.198.168:27017/?')  # 替换为URL值
db = client['PharmRG']  # 替换数据库名
collection = db['DrugBank']  # 替换集合名

# 读取本地Excel文件
df = pd.read_excel('D:/data/fulldata.xlsx', header=None)  # 替换成文件路径

# 遍历每一行数据
for _, row in df.iterrows():
    # 使用正则表达式匹配所有的大括号及里面的内容
    matches = re.findall(r'\{([^}]+)\}', row[0])
    data_list = []  # 存储每一行拆分后的数据项
    for match in matches:
        # 创建一个空字典
        data = {}
        # 利用逗号分割大括号内的内容
        items = match.split(',')
        for item in items:
            # 将冒号前的数据项作为关键字，冒号后的数据项作为关键字的值
            key_val = item.split(':', maxsplit=1)
            if len(key_val) == 2:  # 确保拆分后存在关键字和值
                key, val = key_val
                data[key.strip()] = val.strip()
        data_list.append(data)  # 将拆分后的数据项存入列表
    # 将数据项插入MongoDB数据库中
    result = collection.insert_one({'data': data_list})
    print(f"插入文档的ID为：{result.inserted_id}")

# 关闭数据库连接
client.close()