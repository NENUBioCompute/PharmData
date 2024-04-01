"""
  -*- encoding: utf-8 -*-
  @Author: zhaojingtong
  @Time  : 2024/03/11 20:53
  @Email: 2665109868@qq.com
  @function
"""
from PharmDataProject.Utilities.Database.dbutils import *
import pickle

cfgfile = '../../conf/drugkb.config'
db = DBconnection(cfgfile, 'PharmRG', 'source_drugbank')
alias = []
co = 0
for i in db.collection.find().limit(5):
    alias.append([i['name']])
    if i['synonyms'] is not None:
        if type(i['synonyms']['synonym']) == list:
            for j in i['synonyms']['synonym']:
                alias[co].append(j['#text'])
        elif type(i['synonyms']['synonym']) == dict:
            alias[co].append(i['synonyms']['synonym']['#text'])
        else:
            print(type(i['synonyms']['synonym']))
            print(i['name'])
            break
    co += 1
    if co % 100 == 0:
        print(co)
print(alias)
# output_file = open('./pickles/standard_drugbank.pkl', 'wb')
# pickle.dump(alias, output_file)
# output_file.close()
