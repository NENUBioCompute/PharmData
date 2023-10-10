import json
import xmltodict
import os
import zipfile
import shutil

def convert_gpml_to_json(gpml_path):
    with open(gpml_path, encoding="UTF-8") as xml_file:
        # 将xml文件转化为字典类型数据
        parsed_data = xmltodict.parse(xml_file.read())
        # 关闭文件流，其实 不关闭with也会帮你关闭
        xml_file.close()
        # 将字典类型转化为json格式的字符串
        json_conversion = json.dumps(parsed_data, ensure_ascii=False)
        # 将字符串写到文件中
        json_path = os.path.splitext(gpml_path)[0] + '.json'
        with open(json_path, 'w', encoding="UTF-8") as json_file:
            json_file.write(json_conversion)
            json_file.close()

        # 删除原始的gpml文件
        os.remove(gpml_path)


def unzip_and_convert_to_json(zip_dir):
    # 获取指定目录下的所有压缩文件
    zip_files = [f for f in os.listdir(zip_dir) if f.endswith('.zip')]

    for zip_file in zip_files:
        zip_path = os.path.join(zip_dir, zip_file)
        extract_dir = os.path.splitext(zip_path)[0]

        # 解压缩文件
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # 将GPML文件转换为JSON
        gpml_files = [f for f in os.listdir(extract_dir) if f.endswith('.gpml')]
        for gpml_file in gpml_files:
            gpml_path = os.path.join(extract_dir, gpml_file)
            convert_gpml_to_json(gpml_path)

        # 删除原压缩包
        os.remove(zip_path)
