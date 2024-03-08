import os
import re

# 指定目录路径
directory_path = r'C:\Users\win11\PycharmProjects\SMPDB\smpdb_biopax'

# 遍历目录中的所有.owl文件
for filename in os.listdir(directory_path):
    if filename.endswith(".owl"):
        owl_file_path = os.path.join(directory_path, filename)

        # 读取文件内容
        with open(owl_file_path, 'r', encoding='utf-8') as file:
            file_content = file.read()

        # 使用正则表达式替换rdf:ID中的斜杠、短横线和逗号
        updated_content = re.sub(
            r'rdf:ID="([^"]*)"',
            lambda match: 'rdf:ID="' + match.group(1).replace('/', '_').replace('-', '_').replace(',', '_') + '"',
            file_content
        )

        # 将更新后的内容写回文件
        with open(owl_file_path, 'w', encoding='utf-8') as file:
            file.write(updated_content)

        print(f"成功替换 {owl_file_path} 中的斜杠、短横线和逗号。")
