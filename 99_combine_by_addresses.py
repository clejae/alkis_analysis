# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import time
import pandas as pd
import json
import glob
# ------------------------------------------ START TIME ------------------------------------------------------#
stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
print("start: " + stime)
# ------------------------------------------ USER INPUT ------------------------------------------------#
WD = r'C:\Users\IAMO\Documents\work_data\chapter1\\'

IN_PTH = r"tables\ALKIS\04_owner_class_reclassification\unique_owners_stretched_level3_step1_categories_aggr2.csv"

OUT_PTH = r"C:\Users\IAMO\Documents\work_data\chapter1\tables\ALKIS\99_combine_by_addresses"

os.chdir(WD)
# -------------------------------------------------------------------------------------------------------------#

df = pd.read_csv(IN_PTH, sep=';')

level1_dict = {
    1: "private",
    2: "companies",
    3: "non_profit",
    4: "religious",
    5: "public"
}

## for each class of owners, stretch df, so that the owners with multiple addresses occur as multiple entries
j = 1
for j in range(1, 6):
    print("\n", level1_dict[j])
    df_sub = df.loc[df["level1"] == j].copy()
    df_sub = df_sub.drop(columns=['OGC_FID', 'area', 'AMTLFLSFL'])
    df_sub = df_sub.drop_duplicates()
    print("Length df_sub before stretching addresses:", len(df_sub))

    t = df_sub[df_sub['clean_address'].notna()].copy()
    t_na = df_sub[df_sub['clean_address'].isna()].copy()
    t_na['clean_address'] = ''
    t_multi = t.loc[t['clean_address'].str.contains('_')].copy()
    t_single = t.loc[~t['clean_address'].str.contains('_')].copy()

    col_dict = {col: [] for col in t_multi}
    columns = t_multi.columns.drop('clean_address').to_list()
    for i, item in enumerate(list(t_multi['clean_address'])):
        adr_lst = item.split('_')
        for adr in adr_lst:
            for col in columns:
                val = t_multi[col].iloc[i]
                col_dict[col].append(val)
            col_dict['clean_address'].append(adr)

    t_multi = pd.DataFrame(col_dict)

    df_out = pd.concat([t_single, t_multi, t_na])
    print("Length df_out before drop duplicates:", len(df_out))
    if j == 1:
        ## clean owner merge
        df_out['fam_name'] = df_out.apply(lambda row: row.owner_clean.split(',')[0], axis=1)
        df_out['owner_merge_new'] = df_out['fam_name'] + df_out['clean_address']
        df_out['owner_merge_new'] = df_out['owner_merge_new'].str.replace(' ', '')
        df_out['owner_merge_new'] = df_out['owner_merge_new'].str.replace('-', '')
        df_out['owner_merge_new'] = df_out['owner_merge_new'].str.replace(',', '')
        df_out['owner_merge_new'] = df_out['owner_merge_new'].str.replace('&', '')
        df_out['owner_merge_new'] = df_out['owner_merge_new'].str.replace('+', '')
        df_out['owner_merge_new'] = df_out['owner_merge_new'].str.replace('`', '')
    else:
        df_out['owner_merge_new'] = df_out['owner_merge']

    df_out = df_out.drop_duplicates(subset=['owner_merge_new', 'clean_address'])
    df_out.loc[df_out['clean_address'].isna(), 'clean_address'] = ''
    print("Length df_out after drop duplicates:", len(df_out))

    df_out.to_csv(rf"{OUT_PTH}\{j:02d}_{level1_dict[j]}_unique_owners_and_addresses_stretched.csv", index=False, sep=';')

## load separate dataframes
j = 1
df_priv = pd.read_csv(rf"{OUT_PTH}\{j:02d}_{level1_dict[j]}_unique_owners_and_addresses_stretched.csv", sep=';', low_memory=False)
df_priv = df_priv[df_priv['clean_address'].notna()]

j = 2
df_comp = pd.read_csv(rf"{OUT_PTH}\{j:02d}_{level1_dict[j]}_unique_owners_and_addresses_stretched.csv", sep=';')

j = 3
df_nonp = pd.read_csv(rf"{OUT_PTH}\{j:02d}_{level1_dict[j]}_unique_owners_and_addresses_stretched.csv", sep=';')

## For each company get list of private owners with same address and save to json
count = 0
for i, item in enumerate(list(df_comp['clean_address'])):

    item = str(item)
    space_count = item.count(' ')
    comma_count = item.count(',')
    if space_count > 1 and comma_count > 0:
        owner_clean = df_comp["owner_clean"].iloc[i]
        owner_merge = df_comp["owner_merge"].iloc[i]
        clean_address = df_comp["clean_address"].iloc[i]

        json_pth = f'{OUT_PTH}/networks/{owner_merge}.json'
        if os.path.exists(json_pth):
            with open(json_pth) as json_file:
                comp_dict = json.load(json_file)
            if clean_address not in comp_dict['clean_address']:
                comp_dict['clean_address'].append(clean_address)
        else:
            comp_dict = {
                "owner_clean": owner_clean,
                "owner_merge": owner_merge,
                "clean_address": [clean_address],
                "private_owners": [],
                "private_owners_merge": [],
                "private_owners_addr" :[]
            }

        df_sub = df_priv.loc[df_priv['clean_address'] == item].copy()
        if len(df_sub) > 0:
            count += 1
            private_owners = df_sub['owner_clean'].tolist()
            private_owners_merge = df_sub['owner_merge'].tolist()
            private_owners_addr = df_sub['clean_address'].tolist()
            comp_dict["private_owners"] += private_owners
            comp_dict["private_owners_merge"] += private_owners_merge
            comp_dict["private_owners_addr"] += private_owners_addr

            with open(json_pth, "w") as outfile:
                json.dump(obj=comp_dict, fp=outfile, indent=4, ensure_ascii=True)

## Sort json by number of private owners
lst = glob.glob(f'{OUT_PTH}/networks/*.json')
for json_pth in lst:
    with open(json_pth) as json_file:
        comp_dict = json.load(json_file)
    num_priv_own = len(comp_dict['private_owners'])

    json_folder = f'{OUT_PTH}/networks/{num_priv_own}'
    if not os.path.exists(json_folder):
        os.makedirs(json_folder)
    file_name = os.path.basename(json_pth)
    json_out = f'{OUT_PTH}/networks/{num_priv_own}/{file_name}'
    with open(json_out, "w") as outfile:
        json.dump(obj=comp_dict, fp=outfile, indent=4, ensure_ascii=True)