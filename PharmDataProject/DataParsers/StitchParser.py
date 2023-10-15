import json
import os

def parse_txt(input_file):
    data = []
    with open(input_file, 'r') as file:
        for line in file:
            parts = line.strip().split('\t')
            if len(parts) >= 3:
                item = {
                    'protein1': parts[0],
                    'protein2': parts[1],
                    'score': parts[2],
                    'annotation': parts[3] if len(parts) >= 4 else ''
                }
                data.append(item)
    return data

input_file = "protein.info.v12.0.txt"
parsed_data = parse_txt(input_file)