"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2023/10/12 15:28
  @Email: 2665109868@qq.com
  @function
"""
import json

class DrugMap:
    def get_paragraphs(self, file_path):
        with open(file_path, "r") as file:
            content = file.read()
            paragraphs = content.split("\n\n")
            return paragraphs

    def get_keywords(self, paragraph):
        lines = paragraph.split("\n")
        keys = {}
        for line in lines:
            key = line.strip().split(":")[0]
            keys[key] = ""
        return keys

    def get_informatin(self, file_path):
        paragraphs = self.get_paragraphs(file_path)
        keywords = self.get_keywords(paragraphs[1])
        num = len(paragraphs)
        print(num)
        for i in range(2, num):
            keyword = keywords
            lines = paragraphs[i].split("\n")
            print(lines)
            for line in lines:
                description, key, value = line.strip().split("\t")
                keyword[key] = [v.strip() for v in value.split(";")] if ";" in value else value
            with open(file_path[:-4]+"/"+str(i-2)+".json", "w") as file:
                json.dump(keyword, file)
            file.close()

drugmap = DrugMap()
drugmap.get_informatin("09 Molecular Interaction Atlas of All Drug.txt")
