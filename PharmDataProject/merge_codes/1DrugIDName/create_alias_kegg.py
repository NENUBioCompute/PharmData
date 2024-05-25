from PharmDataProject.Utilities.Database.dbutils import *
import pickle

cfgfile = '../../conf/drugkb.config'

db1 = DBconnection(cfgfile, 'PharmRG', 'source_kegg_drugs')
alias1 = []
for i in db1.collection.find():
    if 'NAME' not in i:
        continue
    tmpa = i['NAME']
    if isinstance(tmpa, list):
        tmpa = [j.split('(')[0].strip() for j in tmpa]
        tmpa = [j.split(';')[0].strip() for j in tmpa]
        tmpa = list(set(tmpa))
        # print(tmpa)
        alias1.append(tmpa)
    else:
        tmpa = [tmpa.split('(')[0].strip().split(';')[0].strip()]
        alias1.append(tmpa)
        # print(tmpa)
    # alias1.append(tmpa)
# print(alias1)
output_file = open('./pickles/new_alias_kegg_drugs.pkl', 'wb')
pickle.dump(alias1, output_file)
output_file.close()