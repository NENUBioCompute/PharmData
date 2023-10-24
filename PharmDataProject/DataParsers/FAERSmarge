import os
import pandas as pd
import warnings
from datetime import datetime

# local directory to save files.
data_dir = "FAERSdata"
directoryPath = os.getcwd() + '/' + data_dir

data_merge_dir = "FAERSdataMerge"  # directory to save merged file
merge_dir_path = os.getcwd() + '/' + data_merge_dir
if not os.path.isdir(data_merge_dir):
    os.makedirs(data_merge_dir)

merge_file_name = "faersDataLightGBM.csv"

# ignore warnings
warnings.filterwarnings('ignore')


def mergeData():
    # merge all files(DEMO, DRUG, REAC, OUTC) in data_dir
    print("Merge files.\t" + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    try:
        demo = pd.DataFrame(columns=['primaryid', 'caseid', 'age', 'sex', 'wt'])
    except KeyError:
        demo = pd.DataFrame(columns=['primaryid', 'caseid', 'age', 'gndr_cod', 'wt'])
    drug = pd.DataFrame(columns=['primaryid', 'caseid', 'role_cod', 'drugname'])
    reac = pd.DataFrame(columns=['primaryid', 'caseid', 'pt'])
    indi = pd.DataFrame(columns=['primaryid', 'caseid', 'indi_drug_seq', 'indi_pt'])
    try:
        outc = pd.DataFrame(columns=['primaryid', 'caseid', 'outc_cod'])
    except KeyError:
        outc = pd.DataFrame(columns=['primaryid', 'caseid', 'outc_code'])
    for filename in os.listdir(directoryPath):
        if "DEMO" in filename.upper() and "CSV" in filename.upper():
            print("Loading " + filename)
            demo_df = pd.read_csv(directoryPath + "/" + filename, low_memory=False, on_bad_lines='warn')
            demo_df = demo_df[1:]
            demo = demo.append(demo_df, ignore_index=True, sort=None)
        if "DRUG" in filename.upper() and "CSV" in filename.upper():
            print("Loading " + filename)
            drug_df = pd.read_csv(directoryPath + "/" + filename, low_memory=False, on_bad_lines='warn')
            drug_df = drug_df[1:]
            drug = drug.append(drug_df, ignore_index=True, sort=None)
        if "OUTC" in filename.upper() and "CSV" in filename.upper():
             print("Loading " + filename)
             outc_df = pd.read_csv(directoryPath + "/" + filename, low_memory=False, on_bad_lines='warn')
             outc_df = outc_df[1:]
             outc = outc.append(outc_df, ignore_index=True, sort=None)
        if "REAC" in filename.upper() and "CSV" in filename.upper():
            print("Loading " + filename)
            reac_df = pd.read_csv(directoryPath + "/" + filename, low_memory=False, on_bad_lines='warn')
            reac_df = reac_df[1:]
            reac = reac.append(reac_df, ignore_index=True, sort=True)
        if "INDI" in filename.upper() and "CSV" in filename.upper():
            print("Loading " + filename)
            indi_df = pd.read_csv(directoryPath + "/" + filename, low_memory=False, on_bad_lines='warn')
            indi_df = indi_df[1:]
            indi = indi.append(indi_df, ignore_index=True, sort=True)

    # merge files based on primary report id and case id
    demo_drug_df = pd.merge(demo, drug, on=('primaryid', 'caseid'), how='inner')
    demo_drug_outc_df = pd.merge(demo_drug_df, outc, on=('primaryid', 'caseid'), how='inner')
    demo_drug_reac_df = pd.merge(demo_drug_df, reac, on=('primaryid', 'caseid'), how='inner')
    demo_drug_indi_df = pd.merge(demo_drug_df, indi, on=('primaryid', 'caseid'), how='inner')

    # save file
    demo_drug_outc_df.to_csv(merge_dir_path + '/' + merge_file_name, header=True, index=False)
    demo_drug_reac_df.to_csv(merge_dir_path + '/' + merge_file_name, header=True, index=False)
    demo_drug_indi_df.to_csv(merge_dir_path + '/' + merge_file_name, header=True, index=False)
    print("Merge files done!\t" + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


def main():
    mergeData(data_dir)
