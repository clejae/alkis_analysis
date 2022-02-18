import os
import pandas as pd

WD = r"C:\Users\IAMO\Documents\work_data\chapter1\ALKIS\05_calculate_geolocation"

ALKIS_PTH = "05_owners_stretched_addresses_geolocated.csv"
ALKIACS_PTH = "id_list_iacs.txt"

os.chdir(WD)

df = pd.read_csv(ALKIS_PTH, sep=';')
id_lst = pd.read_csv(ALKIACS_PTH)

df['IACS'] = 0
df.loc[df['OGC_FID'].isin(id_lst['OGC_FID']), 'IACS'] = 1

df_sub = df.loc[df['NAME_1'] == 'Brandenburg'].copy()
df_sub = df_sub.loc[df_sub['IACS'] == 1].copy()
df_sub.drop_duplicates(subset=['owner_clean', 'clean_address'], inplace=True)
df_sub = df_sub.loc[df_sub['geocoding'] != 'fuzzy_matching'].copy()
df_sub = df_sub.loc[df_sub['geocoding'] != 'osm_level2'].copy()
df_sub = df_sub.loc[df_sub['full_address'] == 7].copy()
df_sub = df_sub.loc[df_sub['address'].str.count('_') == 0].copy()
df_sub1 = df_sub.loc[df_sub['level1'] != 1].copy()
df_sub2 = df_sub.loc[df_sub['level1'] == 1].copy()
df_sub2 = df_sub2.loc[df_sub2['owner_clean'].str.count('\*') == 1].copy()
# df_sub2['owner_clean'] = df_sub2.apply(lambda row: row.owner_clean.split('*')[0], axis=1)
df_sub = pd.concat([df_sub1, df_sub2], axis=0)
# df_sub = df_sub.loc[df_sub['own_num'] == 1].copy()
df_sub['clean_address'] = df_sub['clean_address'].str.replace('str ', 'str. ')

df_sub = df_sub[['owner_clean', 'clean_address', 'EIGENTUEME', 'level1',  'NAME_4']]
df_sub.rename(columns={'level1': 'category'}, inplace=True)

df_sub.to_csv("Eigentümer_und_Adressen.csv", index=False, sep=';')
