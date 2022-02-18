# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import time
import pandas as pd
import re
import jaro

# ------------------------------------------ USER INPUT ------------------------------------------------#
WD = r"C:\Users\IAMO\Documents\work_data\chapter1\ALKIS"

IN_CSV = r"01_clean_owner_strings\01_owners_and_addresses_separated.csv"

OUT_CSV_MANUAL_ADDRESSES_ASSIGNMENT = "02_identify_unique_owners_adress_comb\manual_assignment_of_missing_adresses.csv"
OUT_CSV_MANUAL_OWNER_ASSIGNMENT = "02_identify_unique_owners_adress_comb\manual_assignment_of_missing_owners.csv"
OUT_CSV = r"02_identify_unique_owners_adress_comb\02_owners_and_addresses_stretched.csv"
# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
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


def clean_address(address_str):

    ## clean
    address_str = address_str.replace('ß','ss')
    address_str =  address_str.replace('ä','ae')
    address_str = address_str.replace('ö', 'oe')
    address_str = address_str.replace('ü', 'ue')
    address_str = address_str.replace('.', '')
    address_str = address_str.replace('-', ' ')
    address_str = address_str.replace(':', '')

    address_str = address_str.replace('strasse', 'str')

    addresses = address_str.split('_')
    addresses = [item.strip() for item in addresses]
    addresses = [address for address in addresses if '00000' not in address]
    addresses = [address for address in addresses if 'unbekannt' not in address]
    lst = []

    # address = addresses[1]
    for address in addresses:

        street = identify_street(address)
        if street != '':
            num = re.search('\d{1,4}\s?[a-f]?$', street)
            number = street[int(num.span()[0]): int(num.span()[1])]
            number = number.replace(' ','')
            if number[0] == '0':
                number = number[1:]
            else:
                pass
            street = street[:int(num.span()[0])]
            street = street.strip()

            street = street + ' ' + number + ', '

        plz = identify_plz(address)

        city = identify_city(address)
        city = city.split('/')[0]
        city = clean_city_text(city, search_terms=['bei', 'ot'])

        if (plz + city) != '':
            clean_add = street + plz + ' ' + city
            lst.append(clean_add)

    lst = set(lst)
    if len(lst) > 1:
        sorted_lst = sorted(lst, key=len)
        lst = []

        for i, item in enumerate(sorted_lst[:-1]):
            if not any(item.replace(' ','') in s_item.replace(' ','') for s_item in sorted_lst[i+1:]):
                lst.append(item)
            else:
                pass
        lst.append(sorted_lst[-1])

    lst = '_'.join(lst)

    return lst


def jaro_address(address_str, thresh):

    addresses = address_str.split('_')
    if len(addresses) > 1:
        addresses = [item.strip() for item in addresses]
        addresses = sorted(addresses, key=len)

        address1 = addresses[0]
        address2 = addresses[1]

        # address1 = 'bahnhofstr 26, 16352 basdorf'#'borkumstr 2, 13189 berlin'#'schoenerlinder dorfstr 19, 16348 wandlitz'
        # address2 = 'bahnhofstr 26, 16348 wandlitz'#'hauptstr 120, 16352 berlin'#'schoenerlinder dorfstr 19, 16352 wandlitz'

        jw_value = jaro.jaro_winkler_metric(address1, address2)
        # j_value = SequenceMatcher(None, word, station).ratio()
        if jw_value > thresh:
            out = address2
        else:
            # out = '-'
            out = address_str
    else:
        out = addresses[0]

    return out


def clean_multiple_addresses(text):
    """
    Simple version of cleaning a collection of addresses. Reduced collection of addresses, to all addresses that have a
    good quality. Excludes addresses with unkown parts, then replaces typical words with typical abbreviations and then
    builds a set.
    :param text: Collection of addresses, separated by '_'. String.
    :return:
    """
    lst = text.split('_')
    out_lst = []
    for item in lst:
        if not 'unbekannt' or not 'unkown' in item:
            item = clean_address(item)
            if item != '':
                out_lst.append(item)
    if len(out_lst) >= 1:
        out_lst = '_'.join(list(set(out_lst)))
    else:
        out_lst = 'unbekannt'

    return out_lst

def clean_multiple_addresses2(text):
    """
    More advanced version of cleaning a collection of addresses. Reduced collection of addresses, to all addresses
    that have a good quality. Excludes addresses with unkown parts, then replaces typical words with typical
    abbreviations and then checks if address is complete. Only returns addresses that are most complete.
    :param text: Collection of addresses, separated by '_'. String.
    :return:
    """

    lst = text.split('_')
    out_lst = []
    for item in lst:
        if not 'unbekannt' or not 'unkown' in item:
            item = clean_address(item)
            if item != '':
                out_lst.append(item)
    if len(out_lst) == 1:
        out_lst = '_'.join(list(set(out_lst)))
    elif len(out_lst) > 1:
        df = pd.DataFrame(out_lst, columns=['addresses'])
        df['post_code'] = df['addresses'].apply(identify_plz)
        df.loc[df['post_code'] == '00000', 'post_code'] = ''
        df.loc[df['post_code'] == '0000', 'post_code'] = ''
        df['city'] = df['addresses'].apply(identify_city)
        df['city'] = df['city'].apply(clean_city_text, search_terms=['ot', 'bei'])
        df['street'] = df['addresses'].apply(identify_street)
        df['street'] = df['street'].str.replace('straße', 'str', regex=False)
        df['street'] = df['street'].str.replace('strasse', 'str', regex=False)
        df['street'] = df['street'].str.replace('strasße', 'str', regex=False)
        df['street'] = df['street'].str.replace('str.', 'str', regex=False)
        df['clean_addresses'] = df.apply(lambda row:
                        row.street + ', ' + row.post_code + ' ' + row.city if row.street != '' else
                        row.post_code + ' ' + row.city, axis=1)

        ## check if address is complete
        ## 0: no address, 1: only street, 2: only city, 3: street + city,
        ## 4: only pc, 5: pc+ street, 6: pc + city, 7: full address
        df['sfull'] = 1
        df.loc[df['street'] == '', 'sfull'] = 0  ## uni_adr['sfull'][uni_adr['street'] == ''] = 0

        df['cfull'] = 2
        df.loc[df['city'] == '', 'cfull'] = 0  ## uni_adr['cfull'][uni_adr['city'] == ''] = 0

        df['pfull'] = 4
        df.loc[df['post_code'] == '', 'pfull'] = 0  ## uni_adr['pfull'][uni_adr['post_code'] == ''] = 0

        df['full_address'] = df['sfull'] + df['cfull'] + df['pfull']
        out_lst = df.loc[df['full_address'] == df['full_address'].max(), 'clean_addresses'].tolist()
        out_lst = '_'.join(list(set(out_lst)))
        t = 1
    else:
        out_lst = 'unbekannt'

    return out_lst


def stretch_multiple_owners_to_separate_rows():
    df = pd.read_csv(IN_CSV, sep=';')
    df = df[['OGC_FID', 'EIGENTUEME', 'AMTLFLSFL', 'area', 'owner_names', 'own_num', 'addresses']]

    df_done = df.loc[df['own_num'] == 1].copy()
    df_miss = df.loc[df['own_num'] > 1].copy()

    df_dict = {column: [] for column in df_miss.columns}
    for row in df_miss.itertuples():
        fid = row.OGC_FID
        eigent = row.EIGENTUEME
        area = row.area
        amtlfl = row.AMTLFLSFL
        owner_names = row.owner_names
        owner_names = owner_names.split('|')
        owner_names = [item.strip() for item in owner_names]
        addresses = row.addresses.split('|')
        addresses = addresses

        ## drop owner called 'mehrere', because they should not be identified as an owner
        if '(mehrere)' in owner_names:
            x = owner_names.index('(mehrere)')
            del addresses[x]
            owner_names.remove('(mehrere)')

        num = len(owner_names)
        area = area / num
        amtlfl = amtlfl / num
        for j, owner_name in enumerate(owner_names):
            address = addresses[j].strip()
            df_dict['OGC_FID'].append(fid)
            df_dict['EIGENTUEME'].append(eigent)
            df_dict['area'].append(area)
            df_dict['AMTLFLSFL'].append(amtlfl)
            df_dict['owner_names'].append(owner_name.strip())
            df_dict['addresses'].append(address)
            ## Indicates number of co-owners + owner itself
            df_dict['own_num'].append(num)

    df_uni = pd.DataFrame(df_dict)
    df_uni = pd.concat([df_uni, df_done], axis=0)
    df_uni.sort_values(by='OGC_FID', inplace=True)
    df_uni.to_csv(OUT_CSV, sep=';', index=False)

    ## In some cases the addresses are missing, because the detected owner string included some parts that should not have
    ## been there. I manually assigned the addresses and corrected the owner name
    df_miss_addr = df_uni[df_uni['addresses'] == ''].copy()
    df_miss_addr.drop_duplicates(subset=['owner_names', 'EIGENTUEME'], inplace=True)
    df_miss_addr.loc[df_miss_addr['owner_names'].str.count('verstorben') > 0, 'owner_names'] = 'unbekannt'
    df_miss_addr.loc[df_miss_addr['owner_names'].str.count('verstorben') > 0, 'addresses'] = 'unkown 1, 00000 unkown'
    # df_miss_addr.to_csv(OUT_CSV_MANUAL_ADDRESSES_ASSIGNMENT, sep=';', index=False, encoding='ISO 8859-15')

    ## In some cases with multiple owners, one owner name is missing, because the owner name was similar to a street,
    ## or included "geb." and therefore not considered as an owner name. I manually assigned the missing names & addresses
    df_miss_owners = df_uni.loc[df_uni['owner_names'] == ''].copy()
    df_miss_owners.to_csv(OUT_CSV_MANUAL_OWNER_ASSIGNMENT, sep=';', index=False, encoding='ISO 8859-15')


def assign_missing_addresses_and_owners():
    df_uni = pd.read_csv(OUT_CSV, sep=';')

    ## Read corrected csv
    df_miss_addr = pd.read_csv(OUT_CSV_MANUAL_ADDRESSES_ASSIGNMENT[:-4] + '_corrected.csv', sep=';')
    df_miss_addr['owner_names'] = df_miss_addr['owner_names'].str.lower()
    df_miss_addr['owner_names'] = df_miss_addr['owner_names'].str.strip(',')
    df_miss_addr['owner_names'] = df_miss_addr['owner_names'].str.strip()
    df_miss_addr['addresses'] = df_miss_addr['addresses'].str.lower()
    df_miss_addr['addresses'] = df_miss_addr['addresses'].str.strip(',')
    df_miss_addr['addresses'] = df_miss_addr['addresses'].str.strip()

    ## Do the merging separately to not overwrite already correctly identified addresses
    df_uni.loc[df_uni['addresses'] == 'nan', 'addresses'] = None
    df_done = df_uni[df_uni['addresses'].notna()].copy()
    df_miss = df_uni[df_uni['addresses'].isna()].copy()

    for row in df_miss_addr.itertuples():
        address = row.addresses
        owner = row.owner_names
        df_miss.loc[df_miss['owner_names'] == owner, 'addresses'] = address
        df_miss.loc[df_miss['owner_names'] == owner, 'owner_names'] = owner

    df_uni.loc[df_uni['owner_names'].str.count('verstorben') > 0, 'owner_names'] = 'unbekannt'
    df_uni.loc[df_uni['owner_names'].str.count('verstorben') > 0, 'addresses'] = 'unbekannt'
    df_uni.loc[df_uni['addresses'].isna(), 'addresses'] = 'unbekannt'

    ## Read corrected csv
    df_miss_owners = pd.read_csv(OUT_CSV_MANUAL_OWNER_ASSIGNMENT[:-4] + '_corrected.csv', sep=';', encoding='ISO 8859-15')
    df_miss_owners['owner_names'] = df_miss_owners['owner_names'].str.lower()
    df_miss_owners['owner_names'] = df_miss_owners['owner_names'].str.strip(',')
    df_miss_owners['owner_names'] = df_miss_owners['owner_names'].str.strip()
    df_miss_owners['addresses'] = df_miss_owners['addresses'].str.lower()
    df_miss_owners['addresses'] = df_miss_owners['addresses'].str.strip(',')
    df_miss_owners['addresses'] = df_miss_owners['addresses'].str.strip()

    for row in df_miss_owners.itertuples():
        fid = row.OGC_FID
        new_owner = row.owner_names
        address = row.addresses
        df_uni.loc[(df_uni['owner_names'] == '') & (df_uni['OGC_FID'] == fid), 'addresses'] = address
        df_uni.loc[(df_uni['owner_names'] == '') & (df_uni['OGC_FID'] == fid), 'owner_names'] = new_owner

    df_uni = pd.concat([df_miss, df_done], axis=0)
    df_uni.to_csv(OUT_CSV, sep=';', index=False)

def clean_addresses():
    df_uni = pd.read_csv(OUT_CSV, sep=';')
    ## Clean addresses
    df_uni.loc[df_uni['addresses'].str.count('verstorben') > 0, 'owner_names'] = 'unbekannt'
    df_uni.loc[df_uni['owner_names'].str.len() < 5, 'owner_names'] = 'unbekannt'
    df_uni.loc[(df_uni['owner_names'].str.count('verstorben') > 0), 'owner_names'] = 'unbekannt'
    df_uni.loc[df_uni['owner_names'].isna(), 'owner_names'] = 'unbekannt'
    df_uni.loc[df_uni['owner_names'] == 'unbekannt', 'addresses'] = 'unbekannt'
    df_uni['clean_address'] = df_uni['addresses']

    ## Split between entries with one address and entries with multiple addresses
    df_done = df_uni.loc[(df_uni['clean_address'].str.count('_') < 1)].copy()
    df_mult_addresses = df_uni.loc[(df_uni['clean_address'].str.count('_') >= 1)].copy()
    print(len(df_uni), "= done + multiple addresses =", len(df_done) + len(df_mult_addresses))

    df_done['clean_address'] = df_done['clean_address'].str.replace('00000 ', '')
    df_done.loc[df_done['clean_address'].str.count('\*') > 0, 'clean_address'] = 'unbekannt'
    df_done.loc[df_done['clean_address'].str.count('unbekannt') > 0, 'clean_address'] = 'unbekannt'
    df_done.loc[df_done['clean_address'].str.count('verstorben') > 0, 'clean_address'] = 'unbekannt'
    df_done.loc[df_done['clean_address'].str.count('wohnungsgrund') > 0, 'clean_address'] = 'unbekannt'
    df_done.loc[df_done['clean_address'].str.count('unkown') > 0, 'clean_address'] = 'unbekannt'
    df_done.loc[df_done['clean_address'] == '00000', 'clean_address'] = 'unbekannt'
    df_done['clean_address'] = df_done['clean_address'].apply(clean_address)
    df_done.loc[df_done['clean_address'] == '', 'clean_address'] = 'unbekannt'

    df_mult_addresses = df_uni.loc[(df_uni['clean_address'].str.count('_') >= 1)].copy()
    df_mult_addresses['clean_address'] = df_mult_addresses['clean_address'].apply(clean_multiple_addresses)
    df_done2 = df_mult_addresses.loc[(df_mult_addresses['clean_address'].str.count('_') < 1)].copy()

    df_mult_addresses2 = df_mult_addresses.loc[(df_mult_addresses['clean_address'].str.count('_') >= 1)].copy()
    df_red = df_mult_addresses2.drop_duplicates(subset=['owner_names', 'addresses']).copy()
    df_red['clean_address'] = df_red['clean_address'].apply(clean_multiple_addresses2)
    cols_l = ['OGC_FID', 'EIGENTUEME', 'AMTLFLSFL', 'area', 'owner_names', 'own_num', 'addresses']
    cols_r = ['owner_names', 'addresses', 'clean_address']
    cols_on = ['owner_names', 'addresses']
    df_mult_addresses3 = pd.merge(df_mult_addresses2[cols_l], df_red[cols_r], on=cols_on, how='left',)

    df_out = pd.concat([df_done, df_done2, df_mult_addresses3], axis=0)

    # df_uni = pd.concat([df_mult_addresses, df_done], axis=0)
    df_out.sort_values(by='OGC_FID', inplace=True)
    df_out.loc[df_out['owner_names'].str.count("unbekannt") > 0, 'owner_names'] = 'unbekannt'
    df_out.loc[df_out['owner_names'].str.count("unbekannt") > 0, 'clean_address'] = 'unbekannt'
    df_out.to_csv(OUT_CSV, sep=';', index=False)


def calculate_statistics():
    df_uni = pd.read_csv(OUT_CSV, sep=';')

    ## approx. 13,000 owners with 70,000 parcels have multiple addresses
    ## apporx. 49,000 owners with 130,000 parcels have no address

    ## Explore unique owners
    # df_owners = pd.DataFrame(df_uni['owners'].unique(),columns=['owner'])
    # df_owners = df_owners.sort_values(by = ['owner'])
    df_owners_count = df_uni[['owners','OGC_FID']].groupby(['owners']).count()
    df_owners_count = df_owners_count.sort_values(by=['OGC_FID'], ascending=False)
    df_owners = df_owners_count.reset_index()
    df_owners_area = df_uni[['owners', 'area']].groupby(['owners']).sum()
    df_owners_area = df_owners_area.sort_values(by=['area'], ascending=False)
    df_owners_area = df_owners_area.reset_index()
    df_owners_area2 = df_uni[['owners', 'AMTLFLSFL']].groupby(['owners']).sum()
    df_owners_area2 = df_owners_area2.sort_values(by=['AMTLFLSFL'], ascending=False)
    df_owners_area2 = df_owners_area2.reset_index()

    df_owners_uni = pd.merge(df_owners_count, df_owners_area, how='left', on='owners')
    df_owners_uni = pd.merge(df_owners_uni, df_owners_area2, how='left', on='owners')
    df_owners_uni.columns = ['owners', 'p_count', 'area', 'AMTLFLSFL']
    df_owners_uni.to_csv(r'unique_owners2.csv', sep=';', index=False)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)

    os.chdir(WD)
    # stretch_multiple_owners_to_separate_rows()
    # assign_missing_addresses_and_owners()
    clean_addresses()

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()
