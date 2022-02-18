# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import time
import pandas as pd
import geopandas as gpd
import shapely
from geopy.distance import distance


# ------------------------------------------ USER INPUT ------------------------------------------------#
WD = r'C:\Users\IAMO\Documents\work_data\chapter1\ALKIS\\'

## Input
ALKIS_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified.csv"
GEOCODED_ADDRESSES = r"05_calculate_geolocation\05_geocoded_addresses_complete_with_administrative_names_4326.csv"

PARCELS_PTH = r"01_clean_owner_strings\v_eigentuemer_bb_reduced_25832.shp"

## Output
OUTPATH_MISSING = r"05_calculate_geolocation\05_owners_stretched_addresses_not_geolocated.csv"
OUTPATH_FINAL = r"05_calculate_geolocation\05_owners_stretched_addresses_geolocated.csv"
OUTPATH_DISTANCES = r"05_calculate_geolocation\05_owners_stretched_addresses_geolocated_distances.csv"
# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def combine_alkis_addresses_and_geometries(df_alkis, df_addr, addr_col_left, addr_col_right, geocoding=None,
                                           subset_addr_df=None):
    print(f"\nGeocoding: {geocoding}")
    print(f"Alkis merge column: {addr_col_left}; Address merge column: {addr_col_right}")

    if geocoding:
        df_addr = df_addr.loc[df_addr['geocoding'] == geocoding]

    if subset_addr_df:
        df_addr = df_addr[subset_addr_df]

    df_succ = pd.DataFrame(columns=list(df_alkis.columns))

    if df_addr.empty:
        print(f'There are no addresses that were geocoded with {geocoding}!')
        return df_succ, df_alkis

    if addr_col_right not in df_addr.columns:
        print(f'{addr_col_right} is not in columns of df_addr!')
        return df_succ, df_alkis

    if addr_col_left not in df_alkis.columns:
        print(f'{addr_col_left} is not in columns of df_alkis!')
        return df_succ, df_alkis

    df_addr.drop_duplicates(subset=[addr_col_right], inplace=True)

    df_addr['merge_address'] = df_addr[addr_col_right].str.replace(' ', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('.', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace(',', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('-', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('/', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('ä', 'ae', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('ü', 'ue', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('ö', 'oe', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('ß', 'ss', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('asse', '', regex=False)

    df_alkis['merge_address'] = df_alkis[addr_col_left].str.replace(' ', '', regex=False)
    df_alkis['merge_address'] = df_alkis['merge_address'].str.replace('.', '', regex=False)
    df_alkis['merge_address'] = df_alkis['merge_address'].str.replace(',', '', regex=False)
    df_alkis['merge_address'] = df_alkis['merge_address'].str.replace('-', '', regex=False)
    df_alkis['merge_address'] = df_alkis['merge_address'].str.replace('/', '', regex=False)
    df_alkis['merge_address'] = df_alkis['merge_address'].str.replace('ä', 'ae', regex=False)
    df_alkis['merge_address'] = df_alkis['merge_address'].str.replace('ü', 'ue', regex=False)
    df_alkis['merge_address'] = df_alkis['merge_address'].str.replace('ö', 'oe', regex=False)
    df_alkis['merge_address'] = df_alkis['merge_address'].str.replace('ß', 'ss', regex=False)
    df_alkis['merge_address'] = df_alkis['merge_address'].str.replace('asse', '', regex=False)

    df_addr.drop_duplicates(subset=['merge_address'], inplace=True)

    df_merge = pd.merge(df_alkis, df_addr, how='left', on='merge_address')
    df_succ = df_merge.loc[df_merge.geometry.notna()].copy()
    df_fail = df_merge.loc[~df_merge.geometry.notna()].copy()

    df_fail.drop(columns=subset_addr_df + ['merge_address'], inplace=True)
    df_succ.drop(columns=['merge_address'], inplace=True)

    print(f"Number entries original df: {len(df_alkis)}\n"
          f"Number entries success: {len(df_succ)}\n"
          f"Number entries missed: {len(df_fail)}\n"
          f"Original - Success = {len(df_alkis) - len(df_succ)} Difference to missed: {(len(df_alkis) - len(df_succ)) - len(df_fail)}")

    if df_succ.empty:
        print('No entries could be matched!\n')
        df_alkis.drop(columns=['merge_address'], inplace=True)
        return df_succ, df_alkis

    return df_succ, df_fail


def combine_addresses_with_geolocations():
    os.chdir(WD)

    df_alkis = pd.read_csv(ALKIS_PTH, sep=';')

    length_input = len(df_alkis)

    df_addr = pd.read_csv(GEOCODED_ADDRESSES, sep=';', low_memory=False)
    df_addr_na = df_addr.loc[df_addr['geometry'].isna()].copy()
    df_addr.dropna(subset=["geometry"], inplace=True)
    df_addr.rename(columns={"point_addr": "point_address", "full_addre": "full_address"}, inplace=True)

    addr_cols = ['address', 'full_address', 'geometry', 'geocoding', 'point_address', 'fstateofowner', 'parishofowner']


    df_lst = []
    for geocoding in ['google_api', 'nominatim', 'osm_level1']:
        for left_col in ['clean_address']:  #, 'addresses'
            for right_col in ['address', 'point_address']:
                df_succ, df_alkis = combine_alkis_addresses_and_geometries(df_alkis=df_alkis,
                                                                          df_addr=df_addr,
                                                                          addr_col_left=left_col,
                                                                          addr_col_right=right_col,
                                                                          geocoding=geocoding,
                                                                          subset_addr_df=addr_cols)

                if not df_succ.empty:
                    df_out = df_succ.copy()
                    df_lst.append(df_out)

    ## This is done separately and after the others, because for fuzzy matching you should not use point address
    for geocoding in ['fuzzy_matching']:
        for left_col in ['clean_address']:  #, 'addresses'
            for right_col in ['address']:
                df_succ, df_alkis = combine_alkis_addresses_and_geometries(df_alkis=df_alkis,
                                                                          df_addr=df_addr,
                                                                          addr_col_left=left_col,
                                                                          addr_col_right=right_col,
                                                                          geocoding=geocoding,
                                                                          subset_addr_df=addr_cols)

                if not df_succ.empty:
                    df_out = df_succ.copy()
                    df_lst.append(df_out)

    for geocoding in ['osm_level2']:
        for left_col in ['clean_address']:  #, 'addresses'
            for right_col in ['address', 'point_address']:
                df_succ, df_alkis = combine_alkis_addresses_and_geometries(df_alkis=df_alkis,
                                                                          df_addr=df_addr,
                                                                          addr_col_left=left_col,
                                                                          addr_col_right=right_col,
                                                                          geocoding=geocoding,
                                                                          subset_addr_df=addr_cols)

                if not df_succ.empty:
                    df_out = df_succ.copy()
                    df_lst.append(df_out)

    df_miss = df_alkis.dropna(subset=['clean_address']).copy()
    df_miss = df_miss.loc[~df_miss['clean_address'].str.contains('00000')].copy()
    df_miss = df_miss.loc[~df_miss['clean_address'].str.contains('unknown')].copy()
    df_miss = df_miss.loc[~df_miss['clean_address'].str.contains('unbekannt')].copy()
    df_miss.drop_duplicates(subset=['clean_address'], inplace=True)

    df_miss.to_csv(OUTPATH_MISSING, sep=';', index=False)

    print("Addresses with no geolocation:", len(df_miss))

    for col in addr_cols:
        df_alkis[col] = None

    df_lst.append(df_alkis)
    df_out = pd.concat(df_lst, axis=0)

    print(f"Number of original entries: {length_input}\n"
          f"Number of output entries: {len(df_out)}")

    df_out.loc[df_out['geocoding'].isna(), 'geocoding'] = 'not_possible'
    df_out.to_csv(OUTPATH_FINAL, sep=';', index=False)


def wkt_point_distance(wkt1, wkt2):
    # t = pd.DataFrame(gpd_df)
    # wkt1 = t['owner_loc'].iloc[36]
    # wkt2 = t['parcel_loc'].iloc[36]

    if wkt1 == None or wkt2 == None or type(wkt2) == float or type(wkt1) == float:
        dist = None
    else:

        ## extract points
        if type(wkt1) is shapely.geometry.point.Point:
            point1 = wkt1.x, wkt1.y
        elif type(wkt1) is str:
            point1 = wkt1.replace('POINT (', '')
            point1 = point1.replace(')', '')
            point1 = point1.split(' ')
            point1 = float(point1[0]), float(point1[1])

        if type(wkt2) is shapely.geometry.point.Point:
            point2 = wkt2.x, wkt2.y
        elif type(wkt2) is str:
            point2 = wkt2.replace('POINT (', '')
            point2 = point2.replace(')', '')
            point2 = point2.split(' ')
            point2 = (float(point2[0]), float(point2[1]))

        for val in point1:
            if not -90 < val < 90:
                point1 = (0.0, 0.0)
            else:
                pass

        for val in point2:
            if not -90 < val < 90:
                point2 = (0.0, 0.0)
            else:
                pass

        if point1 == (0.0, 0.0) or point2 == (0.0, 0.0):
            dist = None
        else:
            dist = distance(point1, point2).m

    return dist


def calculate_owner_distance_to_parcel():
    print("!! Transform owner_df_pth to shapefile before in QGIS. And transform geometry from EPSG 25832 to 4326 !!")

    owner_loc = pd.read_csv(OUTPATH_FINAL, sep=';')
    parcels = gpd.read_file(PARCELS_PTH)

    print("Calculate centroids of parcels")
    parcels['parcel_loc'] = parcels['geometry'].centroid.to_crs(epsg=4326)
    parcels.rename(columns={'geometry': 'owner_loc'}, inplace=True)

    owner_df = pd.merge(owner_loc, parcels[['OGC_FID', 'parcel_loc']], how='left')
    distance_df = owner_df.drop_duplicates(subset=['owner_merge', 'clean_address']).copy()
    distance_df['distance'] = distance_df.apply(lambda row: wkt_point_distance(row.parcel_loc, row.owner_loc), axis=1)

    owner_df = pd.merge(owner_df, distance_df[['owner_merge', 'clean_address', 'distance']], how='left',
                        on=['owner_merge', 'clean_address'])

    owner_df.to_csv(OUTPATH_DISTANCES, sep=';', index=False)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)


    # combine_addresses_with_geolocations()
    calculate_owner_distance_to_parcel()


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()





