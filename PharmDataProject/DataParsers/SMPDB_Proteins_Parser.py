import zipfile
import os

# 定义ZIP文件路径和解压目标目录
zip_file_path = 'C:/Users/win11/PycharmProjects/SMPDB/SMPDBdownload/smpdb_proteins.csv.zip'
extracted_folder_path = 'C:/Users/win11/PycharmProjects/SMPDB/SMPDBdownload/SMPDB_proteins'


# 创建解压目标目录，如果不存在的话
if not os.path.exists(extracted_folder_path):
    os.makedirs(extracted_folder_path)

# 打开ZIP文件并解压缩到目标目录
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_folder_path)

print(f"成功解压缩文件到 {extracted_folder_path} 目录。")
