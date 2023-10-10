from file import get_file
from dataparser import parse_data
from Mongodb import store_to_mongodb

# 示例调用
file_path = 'D:/data/fulldata.xlsx'
url = 'mongodb://readwrite:readwrite@59.73.198.168:27017/?'
database = 'PharmRG'
collection = 'DrugBank'

df = get_file(file_path)
data_list = parse_data(df)
store_to_mongodb(data_list, url, database, collection)