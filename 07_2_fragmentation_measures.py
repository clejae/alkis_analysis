import geopandas as gpd
import pandas as pd
import json
import time
import os
import numpy as np

WD = r'C:\Users\IAMO\Documents\work_data\chapter1\ALKIS\\'

OWNER_CSV_PTH = r"06_owner_class_aggregation\06_owners_aggregated.csv"
PTH_ALKIS = r"06_owner_class_aggregation\06_owners_aggregated.shp"
PTH_IACS = r"07_analysis\IACS_2020_25832.shp"

INTERSECTION_PTH = r"07_analysis\07_alkis_iacs_intersection.shp"
FARM_INFO_FOLDER = r"07_analysis\farm_information"

def create_folder(directory):
    """
    Tries to create a folder at the specified location. Path should already exist (excluding the new folder).
    If folder already exists, nothing will happen.
    :param directory: Path including new folder.
    :return: Creates a new folder at the specified location.
    """

    import os
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' + directory )

def calc_quantiles(gdf, col_name, num_quantiles, out_pth):
    lst = []
    for i in range(1, num_quantiles+1):
        q = i / (num_quantiles)
        res = gdf[col_name].quantile(q=q)
        lst.append([q, res])
    df1 = pd.DataFrame(lst)
    df1.columns = ['quantile', 'area']
    df1.plot(df1['quantile'], df1['area'])
    df1.to_csv(out_pth)

def gini(x, w=None):
    """
    Found @
    https://stackoverflow.com/questions/48999542/more-efficient-weighted-gini-coefficient-in-python/48999797#48999797

    :param x:
    :param w:
    :return:
    """
    # The rest of the code requires numpy arrays.
    x = np.asarray(x)
    if w is not None:
        w = np.asarray(w)
        sorted_indices = np.argsort(x)
        sorted_x = x[sorted_indices]
        sorted_w = w[sorted_indices]
        # Force float dtype to avoid overflows
        cumw = np.cumsum(sorted_w, dtype=float)
        cumxw = np.cumsum(sorted_x * sorted_w, dtype=float)
        return (np.sum(cumxw[1:] * cumw[:-1] - cumxw[:-1] * cumw[1:]) /
                (cumxw[-1] * cumw[-1]))
    else:
        sorted_x = np.sort(x)
        n = len(x)
        cumx = np.cumsum(sorted_x, dtype=float)
        # The above formula, with all weights equal to 1 simplifies to:
        return (n + 1 - 2 * np.sum(cumx) / cumx[-1]) / n



def calculate_num_owners_per_farm():
    gdf_inters = gpd.read_file(INTERSECTION_PTH)
    df = pd.read_csv(OWNER_CSV_PTH, sep=';')
    df.drop(columns=['AMTLFLSFL', 'area'], inplace=True)

    farms_col = 'BTNR'
    owners_col = 'owner_merge'
    area_col = 'area'
    field_col = 'OBJECTID'
    parc_col = 'OGC_FID'
    categ_col = 'level3'
    distance_col = 'distance'
    agric_col = 'agric'
    owner_loc_col = 'owner_loc'
    name_col = 'owner_clean'

    num_examples = 20

    ## Get unique "Betriebsnummern"
    farm_lst = gdf_inters[farms_col].unique()
    num_farms = len(farm_lst)

    lst = []

    ## Loop over all farms to identify the biggest land owner per farm and per field
    for f, farm_id in enumerate(farm_lst):
        print(f'{f+1}/{num_farms}', farm_id)

        ## create new farm folder
        new_folder = rf'{FARM_INFO_FOLDER}/{farm_id}'
        create_folder(new_folder)

        ## Subset input shape to all fields of current farm
        df_top = gdf_inters[gdf_inters[farms_col] == farm_id]
        df_top = pd.merge(df_top, df, how='left', on='OGC_FID')

        df_top.to_file(fr'{new_folder}\farm_parcel_in_fields_{farm_id}.shp')

        ## Sum of land owners per farm
        df_area = df_top[[area_col, owners_col, categ_col]].groupby(by=[owners_col, categ_col]).sum()
        df_area = df_area.reset_index()
        df_area = df_area.sort_values(area_col, ascending=False)

        ## Sum of land owners per farm
        df_dist = df_top[[distance_col, owners_col, agric_col]]
        df_dist = df_dist.groupby(by=[owners_col, agric_col]).mean()
        df_dist = df_dist.reset_index()

        df_area = pd.merge(df_area, df_dist, how='left', on=owners_col)
        df_area = df_area.sort_values(by=['area'], ascending=False)
        df_area.to_csv(fr'{new_folder}\farm_parcel_in_fields_{farm_id}.csv', sep=';', index=False)

        field_num = len(df_top[field_col].unique())
        farm_area = round(df_top[area_col].sum(), 2)
        owners_num = len(df_top[owners_col].unique())
        owners_per_ha = round(owners_num / (farm_area / 10000), 3)
        owners_per_field = round(owners_num /(field_num), 3)
        parcel_num = len(df_top[parc_col].unique())
        parcel_sizeq25 = round(df_top[area_col].quantile(0.25), 2)
        parcel_sizeq50 = round(df_top[area_col].quantile(0.5), 2)
        parcel_sizeq75 = round(df_top[area_col].quantile(0.75), 2)
        parcel_sizemin = round(df_top[area_col].min(), 2)
        parcel_sizemax = round(df_top[area_col].max(), 2)
        parcel_sizemean = round(df_top[area_col].mean(), 2)
        mean_dist = df_area[distance_col].dropna().mean()
        median_dist = df_area[distance_col].dropna().median()
        x1 = np.array(df_area[area_col])
        gini_farm = gini(x1)
        parcels_per_field = parcel_num / field_num

        main_area = df_area[area_col].max()
        main_share = round((main_area / farm_area) * 100, 2)
        main_id = str(df_area[owners_col][df_area[area_col] == main_area].iloc[0])
        main_cat = str(df_area[categ_col][df_area[owners_col] == main_id].iloc[0])
        main_agri = str(df_area[agric_col][df_area[owners_col] == main_id].iloc[0])
        main_dist = float(df_area[distance_col][df_area[owners_col] == main_id][0])
        main_loc = str(df_top[owner_loc_col][df_top[owners_col] == main_id].iloc[0])
        main_name = str(df_top[name_col][df_top[owners_col] == main_id].iloc[0])

        owner_ids = list(df_area[owners_col][:num_examples])
        # owner_names = list(df_area[name_col][:num_examples])
        owner_areas = list(df_area[area_col][:num_examples])
        owner_shares = [round((own_areas / farm_area) * 100, 2) for own_areas in owner_areas]
        owner_dists = list(df_area[distance_col][:num_examples])
        owner_cats = list(df_area[categ_col][:num_examples])
        owner_agrics = list(df_area[agric_col][:num_examples])

        lst.append([farm_area, owners_num, field_num, parcel_num, owners_per_ha, owners_per_field, parcel_sizeq25,
        parcel_sizeq50, parcel_sizeq75, parcel_sizemin, parcel_sizemax, parcel_sizemean, mean_dist, median_dist,
        gini_farm, parcels_per_field])

        farm_dict = {
            'farm_id': farm_id,
            'farm_area': farm_area,
            'num_land_owners': owners_num,
            'num_fields': field_num,
            'num_parcels': parcel_num,
            'owner_per_ha': owners_per_ha,
            "parcel_sizeq25": parcel_sizeq25,
            "parcel_sizeq50": parcel_sizeq50,
            "parcel_sizeq75": parcel_sizeq75,
            "parcel_sizemin": parcel_sizemin,
            "parcel_sizemax": parcel_sizemax,
            "parcel_sizemean": parcel_sizemean,
            "mean_dist": mean_dist,
            "median_dist": median_dist,
            "gini_farm": gini_farm,
            "parcels_per_field": parcels_per_field,
            'main_owner_1': {
                'id': main_id,
                'name': main_name,
                'area': main_area,
                'share': main_share,
                'dist': main_dist,
                'owner_cat': main_cat,
                'agric': main_agri,
                'geolocation': main_loc
            },
            'main_land_owners': {
                'owner_ids': owner_ids,
                # 'owner_names': owner_names,
                'owner_areas': owner_areas,
                'owner_shares': owner_shares,
                'owner_categories': owner_cats,
                'owner_agricultural': owner_agrics,
                'owner_distances': owner_dists
            }
        }

        json_out = f'{new_folder}/farm_characteristics.json'
        with open(json_out, "w") as outfile:
            json.dump(farm_dict, outfile, indent=4)

    df = pd.DataFrame(lst)
    df.columns = ['farm_area', 'owners_num', 'field_num', 'parcel_num', 'owners_per_ha', 'owners_per_field',
                  "parcel_sizeq25", "parcel_sizeq50", "parcel_sizeq75", "parcel_sizemin", "parcel_sizemax",
                  "parcel_sizemean", "mean_dist", "median_dist", "gini_farm", "parcels_per_field"]
    out_pth = f'farm_characteristics.csv'
    df.to_csv(out_pth, index=False)

    print('Calculation of farm characteristics done!')



def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)

    # gdf_alk = gpd.read_file(PTH_ALKIS)
    # calc_quantiles(gdf=gdf_alk,
    #                col_name='AMTLFLSFL',
    #                num_quantiles=200,
    #                out_pth=r"C:\Users\IAMO\Documents\work_data\chapter1\vector\fragmentation\quantiles_amtliche_flaeche.csv")

    calculate_num_owners_per_farm()


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()
