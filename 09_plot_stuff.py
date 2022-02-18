# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import time
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ------------------------------------------ START TIME ------------------------------------------------------#
stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
print("start: " + stime)
# ------------------------------------------ USER INPUT ------------------------------------------------#
WD = r'C:\Users\IAMO\Documents\work_data\chapter1\\'
OWNER_CAT_PTH = r"tables\ALKIS\05_calculate_geolocation\05_owners_stretched_addresses_geolocated_iacs_dist.csv"
# SHP_PTH = r"shps\alkis_iacs_intersection.shp"

OVERALL_STATISTICS_PTH = r'tables\ALKIS\07_statistics\07_overall_stats_iacs_parcels.xlsx'

def get_largest_owners(input_df, level_col, area_col, distance_col, owners_col, name_col, address_col, cat_col, agri_col, limit=None):
    out_dfs = []
    uni_levels = input_df[level_col].unique()
    uni_levels = list(set(uni_levels))

    for lev in uni_levels:
        sub = input_df[input_df[level_col] == lev].copy()
        sub['parcel_count'] = 1

        df_area = sub[[owners_col, area_col, 'parcel_count']].groupby(owners_col).sum().reset_index()
        df_dist = sub[[owners_col, distance_col]].groupby(owners_col).mean().reset_index()
        df_name = sub[[owners_col, name_col, address_col, cat_col, agri_col]].drop_duplicates(subset=[owners_col]).copy()
        df_out = pd.merge(df_area, df_dist, how='left', on=owners_col)
        df_out = pd.merge(df_out, df_name, how='left', on=owners_col)
        df_out.sort_values(by='area_of_owner', ascending=False, inplace=True)
        df_out[level_col] = lev
        if limit:
            df_out = df_out[:limit]
        out_dfs.append(df_out)

    return out_dfs

# ------------------------------------------ LOAD DATA & PROCESSING ------------------------------------------#

os.chdir(WD)

df = pd.read_csv(OWNER_CAT_PTH, sep=';')

# df = gpd.read_file(SHP_PTH)
# df['Area_new'] = df['geometry'].area
# df = pd.DataFrame(df).drop(columns='geometry')

df['distance'] = df['distance'] / 1000
df.loc[df['distance'] > 800, 'distance'] = 800
df['area_of_owner'] = df['area_of_owner'] / 10000

df_lst = get_largest_owners(input_df=df,
                            level_col='level1',
                            area_col='area_of_owner',
                            distance_col='distance',
                            owners_col='owner_merge',
                            name_col='EIGENTUEME',
                            address_col='clean_address',
                            agri_col='agric',
                            cat_col='level3')

df_plot = pd.concat(df_lst, axis=0)

d = {1: 'Private people', 2: 'Private companies', 3: 'Foundations, clubs etc.', 4: 'Religious inst.', 5: 'Pulbic inst.'}

df_plot2 = df_plot.loc[df_plot['level1'].isin([1, 2, 3, 4, 5])].copy()
df_plot2.sort_values(by=['area_of_owner'], ascending=False, inplace=True)
df_plot2 = df_plot2.loc[df_plot2['area_of_owner'] > 220].copy()
df_plot2 = df_plot2[7:]
df_plot2['level1'] = df_plot2['level1'].map(d)
plot = sns.jointplot(data=df_plot2,
                     x="distance",
                     y="area_of_owner",
                     hue='level1',
                     s=5,
                     xlim=(-100, 900),
                     ylim=(-100, 4600),
                     edgecolor=None)
plot.ax_joint.set_xlabel("Mean distance to parcels [km]")
plot.ax_joint.set_ylabel("Owned areas [ha]")
out_pth = r"C:\Users\IAMO\OneDrive - IAMO\2021_04 - Worshops_Presentations_Meetings\2022_03_07 - EOLab colloq - Chapter 1\largest_owners_dist_area.png"
plot.savefig(out_pth)






