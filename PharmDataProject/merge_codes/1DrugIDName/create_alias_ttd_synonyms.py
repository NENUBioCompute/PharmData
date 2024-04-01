from PharmDataProject.Utilities.Database.dbutils import *
import pickle

cfgfile = '../../conf/drugkb.config'

db1 = DBconnection(cfgfile, 'PharmRG', 'source_ttd_synonyms')
alias1 = []
for i in db1.collection.find():
    if 'SYNONYMS' not in i:
        continue
    tmpa = i['SYNONYMS']['value']
    if type(tmpa) != list:
        tmpa = [tmpa]
    if type(i['DRUGNAME']['value']) == str:
        tmpa.append(i['DRUGNAME']['value'])
    else:
        tmpa.extend(i['DRUGNAME']['value'])
    alias1.append(tmpa)
# for i in range(alias1.__len__()):
#     for j in range(alias1[i].__len__()):
#         alias1[i][j] = alias1[i][j].lower()
for i in range(alias1.__len__()):
    alias1[i] = list(set(alias1[i]))
print(alias1)
# for i in range(10000000):
#     if i >= alias1.__len__():
#         break
#     for j in range(10000000):
#         if j >= alias1[i].__len__():
#             break
#         k = i + 1
#         while True:
#             if k >= alias1.__len__():
#                 break
#             if alias1[i][j] in alias1[k]:
#                 alias1[i].extend(alias1[k])
#                 del alias1[k]
#                 k -= 1
#             k += 1
#     if i % 100 == 0:
#         print(i)
output_file = open('./pickles/new_alias_ttd_synonyms.pkl', 'wb')
pickle.dump(alias1, output_file)
output_file.close()