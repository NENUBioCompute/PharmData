import pandas as pd

def get_file(file_path):
    # 读取本地Excel文件
    df = pd.read_excel(file_path, header=None)
    return df