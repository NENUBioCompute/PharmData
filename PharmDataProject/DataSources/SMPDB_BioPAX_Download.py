import os
import zipfile

# 指定压缩文件路径
zip_file_path = r'C:\Users\win11\PycharmProjects\SMPDB\SMPDBdownload\smpdb_biopax.zip'

# 指定解压目标路径
extracted_path = r'C:\Users\win11\PycharmProjects\SMPDB\SMPDBdownload\smpdb_biopax'

    # 解压所有文件到目标路径
# 创建解压目标路径（如果不存在）
if not os.path.exists(extracted_path):
    os.makedirs(extracted_path)

# 打开压缩文件
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_path)

print(f"成功解压文件到 {extracted_path}。")
