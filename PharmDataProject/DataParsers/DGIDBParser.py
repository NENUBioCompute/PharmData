import os
import configparser

class DGIDBParser:
    def __init__(self, config_path='../conf/drugkb_test.config'):
        self.config = configparser.ConfigParser()
        self.config.read(config_path)
        self.data_paths = [self.config.get('dgidb', f'data_path_{i + 1}') for i in range(int(self.config.get('dgidb', 'data_path_num')))]

    def parse(self, data_path):
        file_list = os.listdir(data_path)
        if not file_list:
            print("No files found in the directory:", data_path)
            return []

        file_path = os.path.join(data_path, file_list[0])
        print(f"Reading file: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()
        except UnicodeDecodeError:
            print(f"Failed to read file with utf-8 encoding: {file_path}")
            try:
                with open(file_path, 'r', encoding='latin1') as file:
                    lines = file.readlines()
            except UnicodeDecodeError:
                print(f"Failed to read file with latin1 encoding: {file_path}")
                return []

        if not lines:
            print(f"No content found in the file: {file_path}")
            return []

        keys = lines[0].strip().split('\t')
        parsed_data = []
        for line in lines[1:]:
            values = line.strip().split('\t')
            # 确保每个键都有值，使用 None 填充缺失的值
            data_dict = {keys[i]: (values[i] if i < len(values) else None) for i in range(len(keys))}
            parsed_data.append(data_dict)

        return parsed_data  # 返回解析的数据字典列表

    def test(self):
        all_data = []
        for data_path in self.data_paths:
            print(f"Processing data path: {data_path}")
            data = self.parse(data_path)
            if data:
                all_data.append(data[0])  # 只保存每个文件的第一个条目
            else:
                print("No data parsed.")
        return all_data


if __name__ == '__main__':
    parser = DGIDBParser()
    test_all_parsed_data = parser.test()
    for data in test_all_parsed_data:
        print(data)  # 打印每个数据路径解析出的第一个条目
