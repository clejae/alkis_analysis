# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import time
import geopandas as gpd
import pandas as pd
import requests
import osr
from osgeo import ogr
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import jaro

# ------------------------------------------ USER INPUT ------------------------------------------------#
WD = r'C:\Users\IAMO\Documents\work_data\chapter1\ALKIS\\'

## Input
ADRESSES_PATH = r"05_calculate_geolocation\05_owners_stretched_addresses_not_geolocated.csv"  #r"05_calculate_geolocation\owner_addresses_missing.csv"
LOC_OSM_PATH = r"05_calculate_geolocation\GER_places_post_code_lev4.shp"
ALREADY_GEOCODED_PATH = r"05_calculate_geolocation\05_geocoded_addresses_preliminary.csv"

## Output
OUTPATH_NOMINATIM_BACKUP = r"05_calculate_geolocation\05_geocoded_addresses_after_nominatim_geocoding_backup.csv"
OUTPATH_GEOCODED_ALL = r"05_calculate_geolocation\05_geocoded_addresses_complete.csv"

OUTPATH_GEOCODED_MISSES_L1 = r"05_calculate_geolocation\05_missing_addresses_after_osm_geocoding_l1.csv"
OUTPATH_GEOCODED_MATCHES_L1 = r"05_calculate_geolocation\05_geocoded_addresses_after_osm_geocoding_l1.csv"
OUTPATH_GEOCODED_MISSES_NOMINATIM = r"05_calculate_geolocation\05_missing_addresses_after_nominatim_geocoding.csv"
OUTPATH_GEOCODED_MATCHES_NOMINATIM = r"05_calculate_geolocation\05_geocoded_addresses_after_nominatim_geocoding.csv"
OUTPATH_GEOCODED_MISSES_FUZZY = r"05_calculate_geolocation\05_missing_addresses_after_fuzzy_geocoding.csv"

ADDRESSES_SHP = r"05_calculate_geolocation\05_geocoded_addresses_complete_4326.shp"
ADMINISTRATIVE_SHP = r"05_calculate_geolocation\GER_adm_bound_lev4_4326.shp"
FINAL_OUT_PTH = r"05_calculate_geolocation\05_geocoded_addresses_complete_with_administrative_names_4326.csv"

# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#


def replace_characters_in_string(text, char_lst, replace_string):

    ## check if string is text
    if type(text) != str:
        text = str(text)

    for char in char_lst:
        text = text.replace(char, replace_string)

    return text


def clean_city_text(text, search_terms, num_words = 2):
    ## e.g. remove all OT extensions (e.g. 'OT Manchow')
    if text != None:

        str_lst = text.split(" ")
        for search_term in search_terms:
            if search_term in str_lst:
                i = str_lst.index(search_term)
                sub_words = str_lst[i:i + num_words]
                sub_words = ' '.join(sub_words)
            else:
                sub_words = ''

            text = text.replace(sub_words, '')
            text = text.strip()

    else:
        pass

    return text


def address_to_coordinates(address):

    API_KEY = 'AIzaSyDHvUOE9sPuEoUGxURanPftrCykvmuJUo4'

    params = {
        'key': API_KEY,
        'address': address
    }

    base_url = 'https://maps.googleapis.com/maps/api/geocode/json?'

    response = requests.get(base_url, params=params)
    response = response.json()

    if response['status'] == 'OK':
        geometry = response['results'][0]['geometry']
        lat = geometry['location']['lat']
        lon = geometry['location']['lng']
        point = 'POINT ({0} {1})'.format(lon, lat)
    else:
        point = None

    return point


def transform_point(in_point, in_sr, out_sr):

    ## Define Coordinate transformation
    source = osr.SpatialReference()
    source.ImportFromEPSG(in_sr)  ## The geocoding provides coordinates in WGS 84
    target = osr.SpatialReference()
    target.ImportFromEPSG(out_sr)  ## ALKIS and IACS are stored in ETRS89 / UTM zone 32
    transform = osr.CoordinateTransformation(source, target)

    point = in_point.replace('POINT (', '')
    point = point.replace(')', '')
    point = point.split(' ')
    lon_orig, lat_orig = float(point[0]), float(point[1])

    ## transform to EPSG 25832. AddPoint order lat lon:
    out_point = ogr.Geometry(ogr.wkbPoint)
    out_point.AddPoint(lat_orig, lon_orig)
    out_point.Transform(transform)
    lat_out = out_point.GetY()
    lon_out = out_point.GetX()
    out_point = 'POINT ({0} {1})'.format(lon_out, lat_out)

    return out_point


def identify_plz(text):
    text = text.lower()

    ## identify street names and postal codes + city name
    lst = text.split(',')
    pc = ''
    for l, sub in enumerate(lst):
        ## look for a postcal code + city name, if there hasn't been found any yet
        if pc == '':
            ## look  for 5-digit number and a subsequent word
            if re.search(r'\d{5} \b\w+\b', sub):
                pc = re.findall(r'\d{5}', sub)[0]
        if pc == '':
            ## if there was no 5-digit number, try a 4-digit number + word (e.g. Switzerland has 4-digit postal codes)
            if re.search(r'\d{4} \b\w+\b', sub):
                pc = re.findall(r'\d{4}', sub)[0]
        if pc == '':
            ## if there was no 5-digit number + city name, try only a 5-digit number
            if re.search(r'\d{5}', sub):
                pc = re.findall(r'\d{5}', sub)[0]

    pc = pc.strip()

    return pc


def identify_street(text):
    text = text.lower()
    text = text.replace('\n', ',')

    ## identify street names and postal codes + city name
    lst = text.split(',')
    street = ''
    for l, sub in enumerate(lst):
        ## only look for street name if there hasn't been found any yet
        ## look for words with subseqent 1-, 2-, 3- or 4-digit numbers
        if street == '':
            if re.search('(?:[^ ]+ ){0,5}\d{1,4}$', sub):
                street = sub
        if street == '':
            if re.search('(?:[^ ]+ ){0,5}\d{1,4} [a-f]$', sub):
                street = sub
        if street == '':
            if re.search('(?:[^ ]+ ){0,5}\d{1,4}[a-f]$', sub):
                street = sub

    street = street.strip()

    return street


def identify_city(text):
    text = text.lower()

    ## identify street names and postal codes + city name
    lst = text.split(',')
    city = ''
    for l, sub in enumerate(lst):
        ## look for a postcal code + city name, if there hasn't been found any yet
        if city == '':
            ## look  for 5-digit number and a subsequent word
            if re.search(r'\d{5} \b\w+\b', sub):
                city = re.findall(r'\D{1,100}', sub)
                city = ' '.join(city)
        if city == '':
            ## if there was no 5-digit number, try a 4-digit number + word (e.g. Switzerland has 4-digit postal codes)
            if re.search(r'\d{4} \b\w+\b', sub):
                city = re.findall(r'\D{1,100}', sub)
                city = ' '.join(city)

    city = city.strip()

    return city


def address_to_coordinates_nominatim(address, out_pth):

    addresses = address.split('_')

    point_lst = []
    addr_lst = []
    for addr in addresses:
        addr = addr.replace('oe', 'ö')
        addr = addr.replace('ae', 'ä')
        addr = addr.replace('ue', 'ü')

        geolocator = Nominatim(user_agent="pdlen583ngkdlrz")
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=0.3)
        location = geocode(addr)

        if location:
            lat = location.latitude
            lon = location.longitude
            point = 'POINT ({0} {1})'.format(lon, lat)
            point_lst.append(point)
            addr_lst.append(addr)
        else:
            point = None

    if point_lst:
        point = point_lst[0]
        addr = addr_lst[0]
    else:
        point = None

    with open(out_pth, "a", encoding='ISO-8859-1') as file:
        file.write(f"{address};{point};{addr}\n")

    return point


def transfer_geometry_from_geocoded_address_via_fuzzy_matching(df_addr, df_miss, plzcol_l, plzcol_r, addrcol_l, addrcol_r, geomcol_l, thresh):
    df_succ = pd.DataFrame(columns=list(df_addr.columns))

    if plzcol_l not in df_addr.columns:
        print(f'{plzcol_l} is not in columns of df_addr!')
        return df_succ

    if plzcol_r not in df_miss.columns:
        print(f'{plzcol_r} is not in columns of df_miss!')
        return df_succ

    if addrcol_l not in df_addr.columns:
        print(f'{addrcol_l} is not in columns of df_addr!')
        return df_succ

    if addrcol_r not in df_miss.columns:
        print(f'{addrcol_r} is not in columns of df_miss!')
        return df_succ

    df_addr[plzcol_l] = df_addr[plzcol_l].astype(str)
    df_miss[plzcol_r] = df_miss[plzcol_r].astype(str)

    df_addr['merge_address'] = df_addr[addrcol_l].str.replace(' ', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('.', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace(',', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('-', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('/', '', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('ä', 'ae', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('ü', 'ue', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('ö', 'oe', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('ß', 'ss', regex=False)
    df_addr['merge_address'] = df_addr['merge_address'].str.replace('asse', '', regex=False)

    df_miss['merge_address'] = df_miss[addrcol_r].str.replace(' ', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('.', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace(',', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('-', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('/', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('ä', 'ae', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('ü', 'ue', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('ö', 'oe', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('ß', 'ss', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('asse', '', regex=False)

    df_miss.drop_duplicates(subset=['merge_address'], inplace=True)

    out_lst = []
    count = 0
    for i, address in enumerate(df_miss['merge_address']):
        plz = df_miss[plzcol_r].iloc[i]
        df_sub = df_addr.loc[df_addr[plzcol_l] == plz].copy()
        df_sub['jaro_winkler'] = df_sub['merge_address'].apply(jaro.jaro_winkler_metric, string2=address)
        df_sub = df_sub.loc[df_sub['jaro_winkler'] >= thresh].copy()
        max_val = df_sub['jaro_winkler'].max()
        df_sub = df_sub.loc[df_sub['jaro_winkler'] == max_val].copy()

        if not df_sub.empty:
            count += 1
            geom = df_sub[geomcol_l].iloc[0]
            point_addr = df_sub[addrcol_l].iloc[0]
            print(count, i, round(max_val, 4), address, '==', df_sub[addrcol_l].iloc[0])

            out_lst.append([address, geom, "fuzzy_matching", point_addr])
            # uni_adr.loc[uni_adr['address'] == address, 'geometry'] = geom
            # uni_adr.loc[uni_adr['address'] == address, 'geocoding'] = 'fuzzy_matching'
            # uni_adr.loc[uni_adr['address'] == address, 'point_address'] = point_addr

    df_succ = pd.DataFrame(out_lst)
    df_succ.columns = [addrcol_r, "geometry", "geocoding", "point_address"]

    return df_succ


def addresses_to_coordinates_with_osm_l1_data():

    ## read clean addresses
    df = pd.read_csv(ADRESSES_PATH, sep=';') #, dtype={'post_code': str})

    ## read localities of Germany
    gdf_loc = gpd.read_file(LOC_OSM_PATH)
    df_loc = pd.DataFrame(gdf_loc)[['geometry', 'fclass', 'plz', 'name']]
    df_loc['plz'] = df_loc['plz'].fillna('')
    df_loc['plz'] = df_loc['plz'].astype('str')

    ## clean localities string
    df_loc['name'] = df_loc['name'].str.lower()
    df_loc['name'] = df_loc['name'].apply(
        replace_characters_in_string, char_lst=['\n'], replace_string=',')
    df_loc['name'] = df_loc['name'].apply(
        replace_characters_in_string, char_lst=[' - '], replace_string=' | ')
    df_loc['name'] = df_loc['name'].apply(
        replace_characters_in_string, char_lst=['\x9a', '\x8a', '(', ')', '/'], replace_string='')
    df_loc['name'] = df_loc['name'].apply(
        replace_characters_in_string, char_lst=['-'], replace_string=' ')
    df_loc['name'] = df_loc['name'].apply(clean_city_text, search_terms=['ot', 'bei', '|'])

    ## create unique identifiers for level 1
    df_loc['pc_city'] = df_loc['plz'] + df_loc['name']
    df_loc['pc_city'] = df_loc['pc_city'].str.replace(' ', '')

    df_loc = df_loc.drop_duplicates(['pc_city'])

    ## identify street, post_code, city in df
    df = df[df['clean_address'].notna()]
    df['street'] = df['clean_address'].apply(identify_street)
    df['post_code'] = df['clean_address'].apply(identify_plz)
    df['city'] = df['clean_address'].apply(identify_city)
    df['address'] = df['clean_address']

    ## prepare df of unique addresses
    uni_adr = df[['street', 'post_code', 'city', 'address']].drop_duplicates(['address'])
    uni_adr.index = range(len(uni_adr['street']))

    ## check if address is complete
    ## 0: no address, 1: only street, 2: only city, 3: street + city,
    ## 4: only pc, 5: pc+ street, 6: pc + city, 7: full address
    uni_adr['sfull'] = 1
    uni_adr.loc[uni_adr['street'] == '', 'sfull'] = 0 ## uni_adr['sfull'][uni_adr['street'] == ''] = 0

    uni_adr['cfull'] = 2
    uni_adr.loc[uni_adr['city'] == '', 'cfull'] = 0 ## uni_adr['cfull'][uni_adr['city'] == ''] = 0

    uni_adr['pfull'] = 4
    uni_adr.loc[uni_adr['post_code'] == '', 'pfull'] = 0 ## uni_adr['pfull'][uni_adr['post_code'] == ''] = 0

    uni_adr['full_address'] = uni_adr['sfull'] + uni_adr['cfull'] + uni_adr['pfull']
    uni_adr = uni_adr.drop(columns=['sfull', 'cfull', 'pfull'])
    uni_adr['plz_len'] = uni_adr['post_code'].str.len()

    ## create unique identifier in df of addresses
    uni_adr['pc_city'] = uni_adr['post_code'] + uni_adr['city']
    uni_adr['pc_city'] = uni_adr['pc_city'].str.replace(' ', '')

    ## combine addresses with unique identifier of level 1
    ## divide between matches and misses
    df_comb = pd.merge(uni_adr, df_loc[['pc_city', 'fclass', 'geometry']], how='left', on='pc_city')
    df_clear = df_comb[df_comb['geometry'].notnull()].copy()
    df_clear['geocoding'] = 'osm_level1'
    df_miss = df_comb[df_comb['geometry'].isna()].copy()

    df_clear['point_address'] = df_clear['post_code'] + ' ' + df_clear['city']
    df_clear = df_clear[['street', 'post_code', 'city', 'address', 'full_address', 'plz_len',
                         'pc_city', 'fclass', 'geometry', 'geocoding', 'point_address']]

    df_miss.to_csv(OUTPATH_GEOCODED_MISSES_L1, sep=';', index=False)
    df_clear.to_csv(OUTPATH_GEOCODED_MATCHES_L1, sep=';', index=False)


def addresses_to_coordinates_with_nominatim():
    df_miss = pd.read_csv(OUTPATH_GEOCODED_MISSES_L1, sep=';')
    ## geocode addresses that could not be matched with level 1
    ## dived between matches and misses
    df_miss['geometry'] = df_miss['address'].apply(address_to_coordinates_nominatim, out_pth=OUTPATH_NOMINATIM_BACKUP)

    df_clear_nomi = df_miss[df_miss['geometry'].notnull()].copy()
    df_clear_nomi['geocoding'] = 'nominatim'
    df_clear_nomi['geometry'] = df_clear_nomi['geometry'].apply(transform_point, in_sr=4326, out_sr=25832)

    # point_address_df = pd.read_csv(OUTPATH_NOMINATIM_BACKUP, sep=';', encoding='ISO 8859-1')
    # point_address_df.drop_duplicates(subset='in_address', inplace=True)
    # df_clear_nomi = pd.merge(df_clear_nomi, point_address_df, how='left', left_on='address', right_on='in_address')
    # df_clear_nomi.drop(columns=[' point', 'in_address'], inplace=True)
    # df_clear_nomi.rename(columns={' point_address': 'point_address'}, inplace=True)
    # df_clear_nomi = df_clear_nomi[['street', 'post_code', 'city', 'address', 'full_address', 'plz_len',
    #                                'pc_city', 'fclass', 'geometry', 'geocoding', 'point_address']]

    df_miss_nomi = df_miss[df_miss['geometry'].isnull()].copy()

    # df_clear_nomi.to_csv(OUTPATH_GEOCODED_MATCHES_NOMINATIM, sep=';', index=False)

    df_clear_nomi.drop(columns='fclass', inplace=True)
    df_clear_nomi['point_address'] = df_clear_nomi['address']

    ## open already geocoded addresses and append newly geocoded addresses to them
    df_pre = pd.read_csv(ALREADY_GEOCODED_PATH, sep=';')
    df_pre = pd.concat([df_pre, df_clear_nomi], axis=0)

    df_pre.to_csv(ALREADY_GEOCODED_PATH, sep=';', index=False)
    df_miss_nomi.to_csv(OUTPATH_GEOCODED_MISSES_NOMINATIM, sep=';', index=False)


def address_to_coordinates_fuzzy_matching():

    df_addr = pd.read_csv(ALREADY_GEOCODED_PATH, sep=';')
    df_miss = pd.read_csv(OUTPATH_GEOCODED_MISSES_NOMINATIM, sep=';')

    df_clear = transfer_geometry_from_geocoded_address_via_fuzzy_matching(df_addr=df_addr,
                                                                          df_miss=df_miss,
                                                                          plzcol_l='post_code',
                                                                          plzcol_r='post_code',
                                                                          addrcol_l='point_address',
                                                                          addrcol_r='address',
                                                                          geomcol_l='geometry',
                                                                          thresh=0.93)

    addrcol_r = 'address'
    df_miss['merge_address'] = df_miss[addrcol_r].str.replace(' ', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('.', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace(',', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('-', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('/', '', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('ä', 'ae', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('ü', 'ue', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('ö', 'oe', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('ß', 'ss', regex=False)
    df_miss['merge_address'] = df_miss['merge_address'].str.replace('asse', '', regex=False)

    df_miss.drop_duplicates(subset=['merge_address'], inplace=True)

    df_miss.drop(columns=['fclass', 'geometry'], inplace=True)
    df_miss = pd.merge(df_miss, df_clear, how='left', left_on='merge_address', right_on='address')
    df_clear = df_miss.loc[df_miss['geometry'].notna()].copy()
    df_clear.drop(columns=['address_y', 'merge_address'], inplace=True)
    df_clear.rename(columns={"address_x":"address"}, inplace=True)
    df_addr.drop(columns=['merge_address'], inplace=True)

    df_miss = df_miss.loc[df_miss['geometry'].isna()].copy()
    df_miss.drop(columns=['merge_address', 'address_y', 'geometry', 'geocoding', 'point_address'], inplace=True)
    df_miss.rename(columns={"address_x": "address"}, inplace=True)

    df_addr = pd.concat([df_addr, df_clear], axis=0)
    df_addr.to_csv(ALREADY_GEOCODED_PATH, sep=';', index=False)
    df_miss.to_csv(OUTPATH_GEOCODED_MISSES_FUZZY, sep=';', index=False)


def addresses_to_coordinates_with_osm_l2_data():
    df_miss_nomi = pd.read_csv(OUTPATH_GEOCODED_MISSES_FUZZY, sep=';')

    ## read localities of Germany
    gdf_loc = gpd.read_file(LOC_OSM_PATH)
    df_loc = pd.DataFrame(gdf_loc)[['geometry', 'fclass', 'plz', 'NAME_4']]
    df_loc['plz'] = df_loc['plz'].fillna('')
    df_loc['plz'] = df_loc['plz'].astype('str')

    ## clean level 2 localities string (column name: NAME_4)
    df_loc['NAME_4'] = df_loc['NAME_4'].str.lower()
    df_loc['NAME_4'] = df_loc['NAME_4'].apply(
        replace_characters_in_string, char_lst=['\n'], replace_string=',')
    df_loc['NAME_4'] = df_loc['NAME_4'].apply(
        replace_characters_in_string, char_lst=[' - '], replace_string=' | ')
    df_loc['NAME_4'] = df_loc['NAME_4'].apply(
        replace_characters_in_string, char_lst=['\x9a', '\x8a', '(', ')', '/'], replace_string='')
    df_loc['NAME_4'] = df_loc['NAME_4'].apply(
        replace_characters_in_string, char_lst=['-'], replace_string=' ')
    df_loc['NAME_4'] = df_loc['NAME_4'].apply(clean_city_text, search_terms=['ot', 'bei', '|'])

    ## create unique identifiers for level 2
    df_loc['pc_city_l2'] = df_loc['plz'] + df_loc['NAME_4']
    df_loc['pc_city_l2'] = df_loc['pc_city_l2'].str.replace(' ', '')

    df_loc_l2 = df_loc.drop_duplicates(['pc_city_l2'])

    ## combine misses with unique identifier of level 2
    ## divide between matches and misses
    df_comb_l2 = pd.merge(df_miss_nomi[['street', 'post_code', 'city', 'address', 'full_address', 'plz_len', 'pc_city']],
                          df_loc_l2[['pc_city_l2', 'fclass', 'geometry']],
                          how='left', left_on='pc_city', right_on='pc_city_l2')
    df_clear_l2 = df_comb_l2[df_comb_l2['geometry'].notnull()].copy()
    df_clear_l2['geocoding'] = 'osm_level2'
    df_clear_l2 = df_clear_l2.drop(columns='pc_city_l2')
    df_miss_l2 = df_comb_l2[df_comb_l2['geometry'].isna()].copy()

    df_clear_l2['point_address'] = df_clear_l2['post_code'].astype(int).astype(str) + ' ' + df_clear_l2['city']
    df_clear_l2['geocoding'] = 'osm_level2'

    df_clear_l2 = df_clear_l2[['street', 'post_code', 'city', 'address', 'full_address', 'plz_len',
                               'pc_city', 'geometry', 'geocoding', 'point_address']]

    df_miss_l2 = df_comb_l2[df_comb_l2['geometry'].isna()].copy()
    df_miss_l2['geocoding'] = 'not possible'
    df_miss_l2['point_address'] = None
    df_miss_l2 = df_miss_l2[['street', 'post_code', 'city', 'address', 'full_address', 'plz_len',
                             'pc_city', 'geometry', 'geocoding', 'point_address']]

    ## open already geocoded addresses and append newly geocoded addresses to them
    df_pre = pd.read_csv(ALREADY_GEOCODED_PATH, sep=';')
    df_pre = pd.concat([df_pre, df_clear_l2, df_miss_l2], axis=0)

    df_pre.to_csv(OUTPATH_GEOCODED_ALL, sep=';', index=False)


def combine_addresses_with_administrative_levels():
    print("!! DO THIS ONLY AFTER YOU CONVERTED THE ADDRESSES TO WGS 84 COORDINATES IN QGIS !!")

    shp_addr = gpd.read_file(rf"{ADDRESSES_SHP}")
    shp_adm = gpd.read_file(rf"{ADMINISTRATIVE_SHP}")

    addr_with_adm = gpd.sjoin(shp_addr, shp_adm[['NAME_1', 'NAME_4', 'geometry']], how="inner", op='intersects')

    df_miss = shp_addr.loc[~shp_addr['address'].isin(addr_with_adm['address'])].copy()
    df_miss['NAME_1'] = 'Ausland'
    df_miss['NAME_4'] = 'Ausland'
    df_miss.loc[df_miss['geometry'].isna(), 'NAME_1'] = 'Unbekannt'
    df_miss.loc[df_miss['geometry'].isna(), 'NAME_4'] = 'Unbekannt'

    df_out = pd.DataFrame(addr_with_adm)
    df_out.drop(columns='index_right', inplace=True)

    df_out = pd.concat([df_out, df_miss], axis=0)

    df_out.rename(columns={"NAME_1": "fstateofowner", "NAME_4": "parishofowner"}, inplace=True)

    df_out.to_csv(FINAL_OUT_PTH, sep=';', index=False)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)

    os.chdir(WD)

    # addresses_to_coordinates_with_osm_l1_data()
    # addresses_to_coordinates_with_nominatim()
    # address_to_coordinates_fuzzy_matching()
    # addresses_to_coordinates_with_osm_l2_data()

    ## DO THIS ONLY AFTER YOU CONVERTED THE ADDRESSES TO WGS 84 COORDINATES IN QGIS
    combine_addresses_with_administrative_levels()

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()


