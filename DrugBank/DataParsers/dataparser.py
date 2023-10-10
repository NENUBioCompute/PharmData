import re

def parse_data(df):
    # 使用正则表达式匹配所有的大括号及里面的内容
    matches = df[0].str.findall(r'\{([^}]+)\}')
    data_list = []  # 存储每一行拆分后的数据项
    for match in matches:
        for m in match:
            # 创建一个空字典
            data = {}
            # 利用逗号分割大括号内的内容
            items = m.split(',')
            for item in items:
                # 将冒号前的数据项作为关键字，冒号后的数据项作为关键字的值
                key_val = item.split(':', maxsplit=1)
                if len(key_val) == 2:  # 确保拆分后存在关键字和值
                    key, val = key_val
                    data[key.strip()] = val.strip()
            data_list.append(data)
    return data_list