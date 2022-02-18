# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import time
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt

# ------------------------------------------ START TIME ------------------------------------------------------#
stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
print("start: " + stime)
# ------------------------------------------ USER INPUT ------------------------------------------------#
WD = r'C:\Users\IAMO\Documents\work_data\chapter1\\'
OWNER_CAT_PTH = r"tables\ALKIS\05_calculate_geolocation\05_owners_stretched_addresses_geolocated_iacs_dist.csv"
OWNER_CAT_PTH = r"tables\ALKIS\07_statistics\owners_aggr-iacs_inters-geol_dist.csv"
# SHP_PTH = r"shps\alkis_iacs_intersection.shp"

OVERALL_STATISTICS_PTH = r'tables\ALKIS\07_statistics\07_overall_stats_iacs_parcels.xlsx'

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def create_overall_stats(input_df, level_col, area_col, dist_col, fid_col, owners_col):
    out_df = []
    uni_levels = input_df[level_col].unique()
    for lev in uni_levels:
        sub = input_df[input_df[level_col] == lev].copy()
        num_owners = len(sub[owners_col].unique())
        mean_area = round(sub[[area_col, owners_col]].groupby(owners_col).sum().mean().iloc[0] / 10000, 1)
        med_area = round(sub[[area_col, owners_col]].groupby(owners_col).sum().median().iloc[0] / 10000, 1)
        mean_num = sub[[fid_col, owners_col]].groupby(owners_col).count().mean().iloc[0]
        # sub.loc[sub[dist_col] == 999999] = None
        med_dist = round(sub[[dist_col, owners_col]].groupby(owners_col).mean().median().iloc[0] / 1000, 1)
        tot_area = round(sub[area_col].sum() / 10000, 1)
        out_df.append([lev, num_owners, mean_area, med_area, med_dist, mean_num, tot_area])

    out_df = pd.DataFrame(out_df)
    out_df.columns = ['Category', 'Number of owners', 'Mean area per owner [ha]', 'Median area per owner [ha]', 'Median distance [km]', 'Mean number of land parcels per owner', 'Total [ha]']
    out_df = out_df.sort_values(by=['Category'])
    return out_df

def get_largest_owners(input_df, level_col, area_col, distance_col, owners_col, name_col, additional_cols, limit=None):
    out_dfs = []
    uni_levels = input_df[level_col].unique()
    uni_levels = list(set(uni_levels))
    for lev in uni_levels:
        sub = input_df[input_df[level_col] == lev].copy()
        sub['parcel_count'] = 1
        df_area = sub[[owners_col, area_col, 'parcel_count']].groupby(owners_col).sum().reset_index()
        df_dist = sub[[owners_col, distance_col]].groupby(owners_col).mean().reset_index()
        df_name = sub[[owners_col, name_col] + additional_cols].drop_duplicates(subset=[owners_col]).copy()
        df_out = pd.merge(df_area, df_dist, how='left', on=owners_col)
        df_out = pd.merge(df_out, df_name, how='left', on=owners_col)
        df_out.sort_values(by=area_col, ascending=False, inplace=True)
        if limit:
            df_out = df_out[:limit]
        out_dfs.append(df_out)

    return out_dfs

def plot(df, level_col, area_col, distance_col, owners_col, name_col, address_col, cat_col, agri_col):

    fig, ax = plt.subplot()

    ax.hist(df['area_col'])

# ------------------------------------------ LOAD DATA & PROCESSING ------------------------------------------#
os.chdir(WD)

df = pd.read_csv(OWNER_CAT_PTH)

df.loc[df['num_owners_parcel'] > 1, 'level1'] = 6

df.loc[df['distance'] == 999999] = None
# df = gpd.read_file(SHP_PTH)
# df['Area_new'] = df['geometry'].area
# df = pd.DataFrame(df).drop(columns='geometry')

AREA = 'area'  #'AMTLFLSFL', 'Area_new', 'area'
FID = 'OGC_FID'
DISTANCE = 'distance'
OWNERS = 'owner_merge'  #'owner

df_stats_l1 = create_overall_stats(df, 'level1', AREA, DISTANCE, FID, OWNERS)
df_stats_l2 = create_overall_stats(df, 'level2', AREA, DISTANCE, FID, OWNERS)
df_stats_l3 = create_overall_stats(df, 'level3', AREA, DISTANCE, FID, OWNERS)

num_owners = len(df[OWNERS].unique())
mean_area = round(df[[AREA, OWNERS]].groupby(OWNERS).sum().mean().iloc[0] / 10000, 1)
mean_num = df[[FID, OWNERS]].groupby(OWNERS).count().mean().iloc[0]
med_dist = round(df[[DISTANCE, OWNERS]].groupby(OWNERS).mean().median().iloc[0] / 1000, 1)
tot_area = round(df[AREA].sum() / 10000, 1)
med_area = round(df[[AREA, OWNERS]].groupby(OWNERS).sum().median().iloc[0] / 10000, 1)
df_stats_gen = pd.DataFrame([['Category', 'Number of owners', 'Mean area per owner [ha]', 'Median area per owner [ha]', 'Median distance to parcels [km]',  'Mean number of land parcels per owner', 'Total [ha]'], ['Overall', num_owners, mean_area, med_area, med_dist, mean_num, tot_area]])
new_header = df_stats_gen.iloc[0]
df_stats_gen = df_stats_gen[1:]
df_stats_gen.columns = new_header

## Export all category combinations and their respective owner strings
writer = pd.ExcelWriter(OVERALL_STATISTICS_PTH)
for c, df_out in enumerate([df_stats_gen, df_stats_l1, df_stats_l2, df_stats_l3]):
    name = ['Overall', 'level1', 'level2', 'level3'][c]
    df_out.to_excel(writer, sheet_name=name, index=False)

df_lst = get_largest_owners(input_df=df,
                            level_col='level1',
                            area_col='area',
                            distance_col='distance',
                            owners_col='owner_merge',
                            name_col='EIGENTUEME',
                            additional_cols=['clean_address', 'agric', 'level3', 'owner_clean'],
                            limit=1000)

uni_levels = df['level1'].unique()
uni_levels = list(set(uni_levels))
for c, df_out in enumerate(df_lst):
    name = f"Lv1_Class{uni_levels[c]}"
    df_out.to_excel(writer, sheet_name=name, index=False)
writer.save()

df_lst = get_largest_owners(input_df=df,
                            level_col='level1',
                            area_col='area',
                            distance_col='distance',
                            owners_col='owner_merge',
                            name_col='EIGENTUEME',
                            additional_cols=['clean_address', 'agric', 'level3', 'owner_clean'])

for c, df_out in enumerate(df_lst):
    uni_levels = df['level1'].unique()
    uni_levels = list(set(uni_levels))
    name = f"Lv1_Class{uni_levels[c]}"
    # out_pth = rf"{os.path.dirname(OVERALL_STATISTICS_PTH)}/{name}.xlsx"
    # writer = pd.ExcelWriter(out_pth)
    # df_out.to_excel(writer, sheet_name=name, index=False, encoding='utf-8')
    # writer.save()
    out_pth = rf"{os.path.dirname(OVERALL_STATISTICS_PTH)}/{name}.csv"
    df_out.to_csv(out_pth, index=False, sep=';', encoding='iso-8859-1')

# ------------------------------------------ END TIME --------------------------------------------------------#
etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
print("start: " + stime)
print("end: " + etime)
# ------------------------------------------ UNUSED BUT USEFUL CODE SNIPPETS ---------------------------------#
