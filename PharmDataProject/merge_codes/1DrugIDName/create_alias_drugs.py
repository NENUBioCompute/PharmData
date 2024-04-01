from PharmDataProject.Utilities.Database.dbutils import *
import pickle

cfgfile = '../../conf/drugkb.config'

db1 = DBconnection(cfgfile, 'PharmRG', 'source_drugs')
alias1 = []
co = 0
for i in db1.collection.find():
    alias1.append([i['drug_name']])
    if 'generic name' in i:
        if i['generic name']!='' and i['generic name']!=None:
            print(i['generic name'])
            j = i['generic name']
            j = j.strip().split(',')

            j = [k.split('and') for k in j]
            print(j)

            # alias1[co].extend(j)
    # if 'Brand Name' in i:
    #     tmp2 = i['Brand Name'].split(',')
    #     tmp2 = [kk.strip() for kk in tmp2]
    #     alias1[co].extend(tmp2)
    co += 1

print(alias1)
# output_file = open('./pickles/new_alias_drugs.pkl', 'wb')
# pickle.dump(alias1, output_file)
# output_file.close()i['generic name']!=''
