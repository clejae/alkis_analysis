# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import json
from collections import Counter
import pandas as pd
import os
import time
import glob
from collections import defaultdict
import geopandas as gpd

# ------------------------------------------ USER INPUT ------------------------------------------------#
WD = r'C:\Users\IAMO\Documents\work_data\chapter1\ALKIS\\'

## Agricultural information pathes
ALKIS_PTH = r"05_calculate_geolocation\05_owners_stretched_addresses_geolocated_distances.csv"
AGRI_COMP_PTH = r"06_owner_class_aggregation\matching_results_alkis_farmsubs_futtermittel.csv"
DAFNE_SEARCH_PTH = r"08_prepare_dafne_search"

DAFNE_AGRIC_PTH = r"06_owner_class_aggregation\matching_results_alkis_dafne_agriculture.csv"
OWNER_CAT_PTH =  r"06_owner_class_aggregation\06_owners_stretched.csv"

## Aggregation pathes
CLASS_IDS_PTH = r'class_ids_l3_preliminary.json'
CLASSIFIER_PTH = r'class_ids_classifier.json'

BACKUP_PTH = r"06_owner_class_aggregation\06_owners_categories_combined_backup-version.csv"
OUT_PTH = r"06_owner_class_aggregation\06_owners_aggregated.csv"

## Combination pathes
PARCELS_PTH = r"01_clean_owner_strings\v_eigentuemer_bb_reduced_25832.shp"
PARCELS_OUT_PTH = r"06_owner_class_aggregation\06_owners_aggregated.shp"
# ------------------------------------------ LOAD DATA & PROCESSING ------------------------------------------#


def combine_owners_with_agricultural_information():
    ## Read ALKIS data and agricultural companies from "farmsubsidies.org" and "Futtermittelbetriebe"
    owner_df = pd.read_csv(ALKIS_PTH, sep=';')
    df_subs_futt = pd.read_csv(AGRI_COMP_PTH, sep=';')

    ## Read already matched companies
    lst = glob.glob(rf"{WD}\{DAFNE_SEARCH_PTH}\*results_v1.xlsx")
    df_lst = [pd.read_excel(pth, sheet_name="Ergebnisse") for pth in lst]
    for i, df in enumerate(df_lst):
        df["ID"] = i
    df_dafne = pd.concat(df_lst, axis=0)
    df_dafne.drop(columns='Unnamed: 0', inplace=True)

    lst = glob.glob(rf"{WD}\{DAFNE_SEARCH_PTH}\*matches_v1.xlsx")
    df_lst = [pd.read_excel(pth) for pth in lst]
    for i, df in enumerate(df_lst):
        df["ID"] = i
    df_matches = pd.concat(df_lst, axis=0)
    df_matches.drop_duplicates(subset='Unternehmensname', inplace=True)
    df_dafne = pd.merge(df_dafne, df_matches, how='left', left_on="BvD ID Nummer", right_on="Gematchte BvD ID")

    occup_dict_prep = {"haupt_codes": df_dafne['WZ 2008 - Haupttätigkeit - Code'].tolist(),
                       "haupt_descr": df_dafne['WZ 2008 - Haupttätigkeit - Beschreibung'].tolist(),
                       "neben_codes": df_dafne['WZ 2008 - Nebentätigkeit - Code'].tolist(),
                       "neben_descr": df_dafne['WZ 2008 Nebentätigkeit - Beschreibung'].tolist()}

    occup_dict_prep["neben_codes"] = [x for x in occup_dict_prep["neben_codes"] if str(x) != 'nan']
    occup_dict_prep["neben_descr"] = [x for x in occup_dict_prep["neben_descr"] if str(x) != 'nan']

    occup_dict_prep["neben_codes"] = [item.split('\n') for item in occup_dict_prep["neben_codes"]]
    occup_dict_prep["neben_descr"] = [item.split('\n') for item in occup_dict_prep["neben_descr"]]

    ## Flatten list of lists
    occup_dict_prep["neben_codes"] = [item for sublist in occup_dict_prep["neben_codes"] for item in sublist]
    occup_dict_prep["neben_descr"] = [item for sublist in occup_dict_prep["neben_descr"] for item in sublist]

    occup_dict = {"codes": occup_dict_prep["haupt_codes"] + occup_dict_prep["neben_codes"],
                  "descr": occup_dict_prep["haupt_descr"] + occup_dict_prep["neben_descr"]}

    occup_df = pd.DataFrame(occup_dict)
    occup_df.drop_duplicates(subset=["codes", "descr"], inplace=True)
    occup_df.dropna(inplace=True)
    occup_df['agric'] = 0
    occup_df['descr'] = occup_df['descr'].str.lower()
    occup_df.loc[occup_df['codes'].str.count('A') > 0, 'agric'] = 1
    occup_df.loc[occup_df['descr'].str.count('fischerei') > 0, 'agric'] = 0
    occup_df.loc[occup_df['descr'].str.count('folz') > 0, 'agric'] = 0
    occup_df.loc[occup_df['descr'].str.count('forst') > 0, 'agric'] = 0
    occup_df.loc[occup_df['descr'].str.count('baum') > 0, 'agric'] = 0
    occup_df.loc[occup_df['descr'].str.count('aquakultur') > 0, 'agric'] = 0
    occup_df.loc[occup_df['descr'].str.count('holz') > 0, 'agric'] = 0
    # print(len(occup_df['codes'].unique()))

    df_dafne = pd.merge(df_dafne, occup_df[["codes", "agric"]], how='left', left_on='WZ 2008 - Haupttätigkeit - Code',
                        right_on="codes")
    df_dafne.rename(columns={'agric': 'agric_haupt'}, inplace=True)

    df_dafne['agric_neben'] = 0
    for code in occup_df.loc[occup_df["agric"] == 1, 'codes']:
        print(code)
        df_dafne.loc[df_dafne['WZ 2008 - Nebentätigkeit - Code'].str.count(code) > 0, "agric_neben"] = 1

    df_dafne.loc[df_dafne['agric_haupt'].isna(), 'agric_haupt'] = 0
    df_dafne.loc[df_dafne['agric_neben'].isna(), 'agric_neben'] = 0
    df_dafne.loc[(df_dafne['agric_neben'] + df_dafne['agric_haupt']) > 0, "agric"] = 1
    df_dafne.loc[df_dafne['agric'].isna(), 'agric'] = 0

    df_dafne.drop_duplicates(subset='Unternehmensname', inplace=True)
    df_dafne.drop(columns=["ID_x", "Nationale ID", "Stadt", "Land", "ID_y", "codes"], inplace=True)
    df_dafne.to_csv(DAFNE_AGRIC_PTH, sep=';', index=False)

    df1 = df_dafne[['Unternehmensname', 'agric']].copy()
    df1.rename(columns={'Unternehmensname': 'owner_clean'}, inplace=True)
    df1 = df1.loc[df1['agric'] == 1].copy()
    dict1 = defaultdict(lambda: 0)
    for row in df1.itertuples():
        dict1[row.owner_clean] = row.agric

    df2 = df_subs_futt[['alkis', 'correct']].copy()
    df2.rename(columns={'correct': 'agric', 'alkis': 'owner_merge'}, inplace=True)
    dict2 = defaultdict(lambda: 0)
    for row in df2.itertuples():
        dict2[row.owner_merge] = row.agric

    owner_df['agric'] = 0
    owner_df['agric'] = owner_df['owner_merge'].map(dict2)
    print(len(owner_df.loc[owner_df['agric'] == 1]))
    owner_df.loc[owner_df['agric'] == 0, 'agric'] = owner_df.loc[owner_df['agric'] == 0, 'owner_clean'].map(dict1)
    print(len(owner_df.loc[owner_df['agric'] == 1]))

    owner_df.drop(columns='address', inplace=True)

    owner_df.to_csv(OWNER_CAT_PTH, sep=';', index=False)


def hierarchical_owner_category_determination(cat_lst, class_ids):
    out = None
    ## Order is important
    for cat_name in ['eg', 'zweck', 'gmbh', 'gmco', 'ag', 'lim', 'ug', 'se', 'agco', 'ugco',
                     'stift', 'gbr', 'kg', 'ohg', 'ev', 'priv', 'bvvg', 'gem', 'land', 'bund', 'kirch']:
        if not out:
            if class_ids[cat_name] in cat_lst:
                out = class_ids[cat_name]
        else:
            pass

    return out


def owner_aggregation():
    ## Read input
    print('Read input')
    with open(CLASS_IDS_PTH) as json_file:
        class_ids = json.load(json_file)
    class_ids_rev = {v: k for k, v in class_ids.items()} ## reverse dictionary

    with open(CLASSIFIER_PTH) as json_file:
        classifier = json.load(json_file)
    df = pd.read_csv(OWNER_CAT_PTH, sep=';')

    if "geometry" in df.columns:
        df.rename(columns={'geometry': 'owner_loc'}, inplace=True)
    # df.loc[df['owners'].isna(), 'owners'] = 'unkown'
    # df.loc[df['owner_merge'].isna(), 'owner_merge'] = 'unkown'

    # df_agr = pd.read_csv(AGRI_COMP_PTH, sep=';')
    #
    # ## Using columns "correct" to indicate which owners in ALKIS are agricultural owners
    # df = pd.merge(df, df_agr[['alkis', 'correct']], how='left', left_on='owner_merge', right_on='alkis')
    # df.drop(columns=['alkis'], inplace=True)
    # df.rename(columns={'correct': 'agric'}, inplace=True)
    # df.loc[df['agric'].isna(), 'agric'] = 0

    print('Group owners and their categories back together')
    fid_lst = list(df['OGC_FID'].unique())
    full_lst = df['OGC_FID'].tolist()

    ## Count occurrence of FIDs
    counter = Counter(full_lst)

    ## Subset df to avoid looping over FIDs that occur only once
    work_lst = [i for i in counter if counter[i] > 1]
    df_work = df.loc[df['OGC_FID'].isin(work_lst)].copy()
    df_done = df.loc[~df['OGC_FID'].isin(work_lst)].copy()
    out_lst = []
    out_dict = {col: [] for col in df.columns}
    # out_dict['num_owners'] = []

    # df_done['num_owners'] = 1

    # fid = 273
    for count, fid in enumerate(work_lst):
        print(f"{count}/{len(work_lst)}")
        ## OGC_FID from loop

        sub = df_work[df_work['OGC_FID'] == fid].copy()

        sub.loc[sub['distance'].isna(), 'distance'] = 9999999

        eigent_lst = sub['EIGENTUEME'].tolist()
        amtfl_lst = sub['AMTLFLSFL'].tolist()
        area_lst = sub['area'].tolist()
        owners_lst = sub['owner_names'].tolist()
        num_owners_lst = sub['own_num'].tolist()
        owner_clean_lst = sub['owner_clean'].tolist()
        cat_lst = sub['category'].tolist()
        owner_merge_lst = sub['owner_merge'].tolist()
        lev3_lst = sub['level3'].tolist()
        lev2_lst = sub['level2'].tolist()
        lev1_lst = sub['level1'].tolist()
        address_lst = sub['addresses'].tolist()
        address_clean_lst = sub['clean_address'].tolist()
        full_adr_lst = sub['full_address'].tolist()
        owner_loc_lst = sub['owner_loc'].tolist()
        geoc_lst = sub['geocoding'].tolist()
        point_adr_lst = sub['point_address'].tolist()
        fstate_lst = sub['fstateofowner'].tolist()
        parish_lst = sub['parishofowner'].tolist()
        parc_loc_lst = sub['parcel_loc'].tolist()
        dist_lst = sub['distance'].tolist()
        # area_of_owner_lst = sub['area_of_owner'].tolist()
        agri_lst = sub['agric'].tolist()

        if len(dist_lst) == 0:
            dist_lst = [9999999 for i in range(len(owners_lst))]

        ## If there are only entities of one category
        if len(set(cat_lst)) == 1:
            ## If there is only one agricultural entity in list, then get all values with the index of this person/comp
            ## If there are multiple, then get all values with the index of entity with shortest distance
            if 1 in agri_lst:
                owner_lst_new = [owner_clean_lst[i] for i, item in enumerate(agri_lst) if item == 1]
                if len(owner_lst_new) == 1:
                    ind = owner_clean_lst.index(owner_lst_new[0])
                else:
                    dist_lst_new = [dist_lst[i] for i, item in enumerate(agri_lst) if item == 1]
                    ind = dist_lst.index(min(dist_lst_new))  #Todo
                    # ind = 0
            else:
                # dist_lst_new = [dist_lst[i] for i, item in enumerate(agri_lst) if item == 1]
                ind = dist_lst.index(min(dist_lst))
                # ind = 0


        ## If there are entites of multiple categories
        else:
            ## Look if they are agricultural entites
            if 1 in agri_lst:
                ## Subset owners to agricultural entites
                owner_lst_new = [owner_clean_lst[i] for i, item in enumerate(agri_lst) if item == 1]
                cat_lst_new = [cat_lst[i] for i, item in enumerate(agri_lst) if item == 1]
                ## If only one owner remains, then get index of entity that is agricultural
                if len(owner_lst_new) == 1:
                    ind = agri_lst.index(1)

                ## If more owners are agricultural then use hierarchical determination of owner category
                ## Of all owner with that category, select the one with the shortest distance
                else:
                    category = hierarchical_owner_category_determination(cat_lst_new, class_ids)
                    dist_lst_new = [dist_lst[i] for i, item in enumerate(cat_lst) if item == category]
                    ind = dist_lst.index(min(dist_lst_new))
                    # ind = cat_lst.index(category)

            ## If no agricultural entities are in the owner list then use hierarchical determination of owner category
            ## Of all owner with that category, select the one with the shortest distance
            else:
                category = hierarchical_owner_category_determination(cat_lst, class_ids)
                ## If all owners are unkown or unlikely, then there will be no match, thus randomly the first one will be taken
                if not category:
                    ind = 0
                else:
                    dist_lst_new = [dist_lst[i] for i, item in enumerate(cat_lst) if item == category]
                    ind = dist_lst.index(min(dist_lst_new))
                # ind = cat_lst.index(category)

        owners_uni = list(set(owner_merge_lst))
        num_owners = len(owners_uni)

        out_dict['OGC_FID'].append(fid)
        out_dict['EIGENTUEME'].append(eigent_lst[0])
        out_dict['AMTLFLSFL'].append(amtfl_lst[0])
        out_dict['area'].append(area_lst[0])
        out_dict['owner_names'].append(' | '.join(owners_lst))
        out_dict['own_num'].append(num_owners)
        out_dict['owner_clean'].append(owner_clean_lst[ind])
        out_dict['category'].append(cat_lst[ind])
        out_dict['owner_merge'].append(owner_merge_lst[ind])
        out_dict['level3'].append(lev3_lst[ind])
        out_dict['level2'].append(lev2_lst[ind])
        out_dict['level1'].append(lev1_lst[ind])
        out_dict['addresses'].append(' | '.join(address_lst))
        out_dict['clean_address'].append(address_clean_lst[ind])
        out_dict['owner_loc'].append(owner_loc_lst[ind])
        out_dict['full_address'].append(full_adr_lst[ind])
        out_dict['geocoding'].append(geoc_lst[ind])
        out_dict['point_address'].append(point_adr_lst[ind])
        out_dict['fstateofowner'].append(fstate_lst[ind])
        out_dict['parishofowner'].append(parish_lst[ind])
        out_dict['parcel_loc'].append(parc_loc_lst[ind])
        out_dict['distance'].append(dist_lst[ind])
        # out_dict['area_of_owner'].append(area_of_owner_lst[ind])
        out_dict['agric'].append(agri_lst[ind])

    df_cat_comb = pd.DataFrame(out_dict)

    df_cat_comb = pd.concat([df_cat_comb, df_done])

    df_cat_comb.sort_values(by='OGC_FID', ascending=True, inplace=True)
    df_cat_comb = df_cat_comb[['OGC_FID', 'owner_clean', 'clean_address',  'owner_merge', 'category', 'level1', 'level2',
                               'level3', 'AMTLFLSFL', 'area', 'distance', 'fstateofowner', 'parishofowner', 'agric',
                               'point_address', 'EIGENTUEME', 'owner_names', 'addresses', 'own_num',  'full_address',
                               'geocoding', 'parcel_loc', 'owner_loc']]

    print(len(df_cat_comb))
    df_cat_comb.to_csv(OUT_PTH, sep=';', index=False)


def combine_parcels_with_owner_information():
    print("Read owner data frame and parcel shapefile.")
    df = pd.read_csv(OUT_PTH, sep=';')
    shp = gpd.read_file(PARCELS_PTH)

    print("Combine owner information and parcel polygons")
    shp = pd.merge(shp[['OGC_FID', 'geometry']], df, how='left', on='OGC_FID')

    print("Write new shapefile to disc.")
    shp.to_file(PARCELS_OUT_PTH)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)


    # combine_owners_with_agricultural_information()
    # owner_aggregation()
    combine_parcels_with_owner_information()


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()