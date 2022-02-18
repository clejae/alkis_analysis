# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import time
import pandas as pd
import json
import jaro
from fuzzywuzzy import fuzz
import datetime


# ------------------------------------------ USER INPUT ------------------------------------------------#
WD = r'C:\Users\IAMO\Documents\work_data\chapter1\ALKIS\\'

OWNERS_PTH = r"03_owner_name_classification\03_owners_stretched_preliminary_classication.csv"
CLASSIFIER_PTH = r'class_ids_classifier.json'

PRIVATE_OWNERS_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified_private-persons.csv"
PRIVATE_OWNERS_GROUPS_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified_private-persons-groups.csv"
PRIVATE_OWNERS_TEMP = r"04_owner_class_reclassification\04_owners_stretched_classified_private-persons-with-multiple-addresses.csv"

COMPANY_OWNERS_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified_private-companies.csv"
COMPANY_OWNERS_TEMP = r"04_owner_class_reclassification\04_owners_stretched_classified_private-companies-with-multiple-addresses.csv"

NONPROF_OWNERS_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified_non-profit-etc.csv"
NONPROF_OWNERS_TEMP = r"04_owner_class_reclassification\04_owners_stretched_classified_non-profit-etc-with-multiple-addresses.csv"

RELIGIOUS_OWNERS_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified_reliqious.csv"
RELIGIOUS_OWNERS_TEMP = r"04_owner_class_reclassification\04_owners_stretched_classified_religious-with-multiple-addresses.csv"

PUBLIC_OWNERS_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified_public.csv"
PUBLIC_OWNERS_TEMP = r"04_owner_class_reclassification\04_owners_stretched_classified_public-with-multiple-addresses.csv"

OUT_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified.csv"   #unique_owners_stretched_level3_step1_categories_aggr2.csv"

STATS_OUT_PTH = r'04_owner_class_reclassification\04_owners_stretched_classified_stats.xlsx'

##ToDo: for companies, non-profit, public and church: decide on an address if there are two or more addresses
## like its done for private people
# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def remove_street(owner_str, lookup_lst):
    if owner_str != None:

        str_lst = owner_str.split(" ")

        if len(str_lst) > 1:
            last = str_lst[-1]
            for search_term in lookup_lst:
                if search_term in last:
                    owner_str = owner_str.replace(last, '')
                    break

    return owner_str


def most_frequent(in_list):
    return max(set(in_list), key=in_list.count)


def jaro_address(address_str, thresh):
    addresses = address_str.split('_')
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

    return out


def fuzzy_match_token_set_ratio(df, look_col, new_col, freq_col, thresh, search_range):
    for i in range(len(df)):
        num_rows = len(df)
        add = search_range
        if i > (num_rows - search_range):
            add = num_rows - i
        df_sub = df[i:i + add].copy()
        df_sub['index_old'] = df_sub.index
        curr_owner = df_sub[look_col][df_sub['index_old'] == i].iloc[0]
        other_owners = list(df_sub[look_col][df_sub['index_old'] != i])

        lst = []
        ind_lst = [i]
        for j, oth_owner in enumerate(other_owners):
            match_value = fuzz.token_set_ratio(curr_owner, oth_owner)
            if (match_value > thresh):  # & (match_value <= 98): #.98,.99
                # print(oth_owner)
                lst.append(oth_owner)
                ind_lst.append(i + (j + 1))

        df_sub2 = df.iloc[ind_lst].copy()
        corr_owner = df_sub2[look_col][df_sub2[freq_col] == df_sub2[freq_col].max()].iloc[0]
        df.loc[ind_lst, new_col] = corr_owner


def create_overall_stats(df, level_col, area_col, fid_col, owners_col):
    out_df = []
    uni_levels = df[level_col].unique()
    for lev in uni_levels:
        sub = df[df[level_col] == lev].copy()
        num_owners = len(sub[owners_col].unique())
        mean_area = round(sub[[area_col, owners_col]].groupby(owners_col).sum().mean().iloc[0] / 1000, 1)
        mean_num = sub[[fid_col, owners_col]].groupby(owners_col).count().mean().iloc[0]
        tot_area = round(sub[area_col].sum() / 1000, 1)
        out_df.append([lev, num_owners, mean_area, mean_num, tot_area])

    out_df = pd.DataFrame(out_df)
    out_df.columns = ['Category', 'Number of owners', 'Mean area per owner [ha]',
                      'Mean number of land parcels per owner', 'Total [ha]']
    out_df = out_df.sort_values(by=['Category'])
    return out_df


def overall_stats_wrapper(df, stats_out_pth, area_col, fid_col, owners_col):
    df_stats_l1 = create_overall_stats(df, 'level1', area_col, fid_col, owners_col)
    df_stats_l2 = create_overall_stats(df, 'level2', area_col, fid_col, owners_col)
    df_stats_l3 = create_overall_stats(df, 'level3', area_col, fid_col, owners_col)

    num_owners = len(df[owners_col].unique())
    mean_area = round(df[[area_col, owners_col]].groupby(owners_col).sum().mean().iloc[0] / 1000, 1)
    mean_num = df[[fid_col, owners_col]].groupby(owners_col).count().mean().iloc[0]
    tot_area = round(df[area_col].sum() / 1000, 1)
    df_stats_gen = pd.DataFrame([['Category', 'Number of owners', 'Mean area per owner [ha]',
                                  'Mean number of land parcels per owner', 'Total [ha]'],
                                 ['Overall', num_owners, mean_area, mean_num, tot_area]])
    new_header = df_stats_gen.iloc[0]
    df_stats_gen = df_stats_gen[1:]
    df_stats_gen.columns = new_header

    ## Export all category combinations and their respective owner strings
    writer = pd.ExcelWriter(stats_out_pth)
    for c, df in enumerate([df_stats_gen, df_stats_l1, df_stats_l2, df_stats_l3]):
        name = ['Overall', 'level1', 'level2', 'level3'][c]
        df.to_excel(writer, sheet_name=name, index=False)
    writer.save()


def get_birthdate(str):
    str_lst = str.split('*')

    if len(str_lst) > 1:
        birthdate = str_lst[1]
        if ',' in birthdate:
            birthdate = birthdate.split(',')[0]
    else:
        birthdate = ''

    return birthdate


def remove_substring_and_followup(text, substring):
    ## e.g. remove all OT extensions (e.g. 'OT Manchow')

    if substring in text:
        start_index = text.find(substring)
        out_text = text[:start_index]
    else:
        out_text = text
    return out_text


def remove_address_part(text, delimiter, address_code_words):
    if delimiter in text:
        ## search for delimiter and get possible address part (i.e the last part of the text)
        text_lst = text.split(delimiter)
        address_part = text_lst[-1]

        ## check if any of the code words is in address part
        ## if so, remove the part from the text
        for address_code in address_code_words:
            if address_code in address_part:
                out_text = ','.join(text_lst[:-1])
                break
            else:
                out_text = text
    else:
        out_text = text

    return out_text


def reclassify_owner_classes():
    print('Reclassify')
    df = pd.read_csv(OWNERS_PTH, sep=';')

    with open(CLASSIFIER_PTH) as json_file:
        classifier = json.load(json_file)

    ## CLASSIFY LEVEL 1 TO 3
    ## Get unique class IDs from df
    uni_class_ids = list(df['category'].astype('str').unique())
    for class_id in uni_class_ids:
        df.loc[df['category'].astype('str') == class_id, 'level3'] = classifier[class_id][0]
        df.loc[df['category'].astype('str') == class_id, 'level2'] = classifier[class_id][1]
        df.loc[df['category'].astype('str') == class_id, 'level1'] = classifier[class_id][2]

    ## Get statistics for original df
    # AREA = 'AMTLFLSFL'
    # FID = 'OGC_FID'
    # OWNERS = 'owner_clean'
    # STATS_OUT_PTH = r'tables\ALKIS\results\01_overall_stats_all_parcels_stretched_owners_before_aggregation2_AMTLFLSFL.xlsx'
    # overall_stats_wrapper(df, STATS_OUT_PTH, AREA, FID, OWNERS)

    ## remove fill words and characters
    df['owner_merge'] = df['owner_clean'].str.replace('mit sitz in ', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace('sitz in ', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace(' mit sitz ', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace(' sitz ', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace(' in ', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace(' ', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace('-', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace(',', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace('&', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace('+', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace('`', '', regex=False)
    df['owner_merge'] = df['owner_merge'].str.replace('\n', '', regex=False)

    df.to_csv(OUT_PTH, sep=";", index=False)


def choose_address_based_on_occurences(df, owner_names, out_pth):

    file = open(out_pth, "w+", encoding='ISO-8859-1')
    file.write("owner_merge;addresses;clean_address\n")

    for owner in owner_names:
        sub = df.loc[df['owner_merge'] == owner].copy()
        addresses = sub['clean_address'].tolist()
        uni_addresses = []
        for address in addresses:
            lst = address.split('_')
            for address in lst:
                if address != 'unbekannt':
                    uni_addresses.append(address)
        if uni_addresses:
            mf_address = most_frequent(uni_addresses)
        else:
            mf_address = 'unbekannt'
            uni_addresses = addresses
        file.write(f"{owner};{'_'.join(list(set(uni_addresses)))};{mf_address}\n")

    file.close()


def clean_company_identifiers():
    print('Private companies')
    df = pd.read_csv(OUT_PTH, sep=';')

    df_comp = df[df['level1'] == 2].copy()

    ## Get df with unique company names and their frequencies (comes in ascending order)
    df_comp_uni = df_comp.groupby(['owner_merge']).size().reset_index(name='Freq').copy()

    ## Fuzzy match company names with forward looking moving window
    fuzzy_match_token_set_ratio(df_comp_uni, 'owner_merge', 'owner_new', 'Freq', 95, 20)

    ## Create dictionary with old names and new names
    own_dict = {}
    # for i in range(len(df_comp_uni)):
    for row in df_comp_uni.itertuples():
        owner_merge = row.owner_merge
        owner_new = row.owner_new
        own_dict[owner_merge] = owner_new

    ## Assign new names based on dictionary
    uni_owners = list(df_comp_uni['owner_merge'].astype('str').unique())
    for uni_own in uni_owners:
        df_comp.loc[df_comp['owner_merge'].astype('str') == uni_own, 'owner_merge'] = own_dict[uni_own]

    ## Decide on one address based on occurences
    print(len(df_comp))
    owner_names = df_comp['owner_merge'].unique().tolist()

    choose_address_based_on_occurences(df=df_comp, owner_names=owner_names, out_pth=COMPANY_OWNERS_TEMP)
    df_assigned = pd.read_csv(COMPANY_OWNERS_TEMP, sep=';', encoding='ISO-8859-1')
    cols_left = ['OGC_FID', 'EIGENTUEME', 'AMTLFLSFL', 'area', 'owner_names', 'own_num', 'owner_clean', 'category',
                 'owner_merge', 'level3', 'level2', 'level1']

    # t = df_assigned.loc[df_assigned['owner_merge'].isin(owner_names)].copy()
    df_out = pd.merge(df_comp[cols_left], df_assigned, how='left', on='owner_merge')
    print(len(df_out))
    df_out.to_csv(COMPANY_OWNERS_PTH, sep=';', index=False)


def clean_non_profit_identifiers():
    df = pd.read_csv(OUT_PTH, sep=';')
    print('Non-profit')
    df_nprof = df[df['level1'] == 3].copy()

    ## Aggregate manually import entities
    df_nprof.loc[df_nprof['owner_merge'].str.count('arbeitersamariter') > 0, 'owner_merge'] = 'arbeitersamariterbund'
    df_nprof.loc[df_nprof['owner_merge'].str.count('arbeiterwohlfahrt') > 0, 'owner_merge'] = 'arbeiterwohlfahrt'
    df_nprof.loc[df_nprof['owner_merge'].str.count('deutschesroteskreuz') > 0, 'owner_merge'] = 'deutschesroteskreuz'
    df_nprof.loc[df_nprof['owner_merge'].str.count('drk') > 0, 'owner_merge'] = 'deutschesroteskreuz'
    df_nprof.loc[
        df_nprof['owner_merge'].str.count('grosstrappenschutz') > 0, 'owner_merge'] = 'fordervereingrosstrappenschutz'
    df_nprof.loc[
        df_nprof['owner_merge'].str.count('heinzsielmann') > 0, 'owner_merge'] = 'heinzsielmannstiftunggutherbigshagen'
    df_nprof.loc[
        df_nprof['owner_merge'].str.count('landesanglerverbandbr') > 0, 'owner_merge'] = 'landesanglerverbandbrandenburgev'
    df_nprof.loc[df_nprof['owner_merge'].str.count(
        'vereinnuth') > 0, 'owner_merge'] = 'landschaftsfoerdervereinnuthenieplitzniederungev'
    df_nprof.loc[df_nprof['owner_merge'].str.count(
        'vereinoberesrhin') > 0, 'owner_merge'] = 'landschaftsfoerdervereinoberesrhinluchev'
    df_nprof.loc[df_nprof['owner_merge'].str.count('nabu') > 0, 'owner_merge'] = 'nabustiftungnationalesnaturerbe'
    df_nprof.loc[
        df_nprof['owner_merge'].str.count('naturschutzbund') > 0, 'owner_merge'] = 'nabustiftungnationalesnaturerbe'
    df_nprof.loc[df_nprof['owner_merge'].str.count('naturschutzfondsbr') > 0, 'owner_merge'] = 'naturschutzfondsbrandenburg'
    df_nprof.loc[df_nprof['owner_merge'].str.count(
        'naturparkschlaubetal') > 0, 'owner_merge'] = 'landschaftspflegeverbadnnaturparkschlaubetal'
    df_nprof.loc[df_nprof['owner_merge'].str.count(
        'edithmaryon') > 0, 'owner_merge'] = 'stiftungedithmaryonzurfoerderungsozialerwohnundarbeitsstaetten'
    df_nprof.loc[df_nprof['owner_merge'].str.count('stiftneuzelle') > 0, 'owner_merge'] = 'stiftungstiftneuzelle'
    df_nprof.loc[df_nprof['owner_merge'].str.count('wwf') > 0, 'owner_merge'] = 'umweltstiftungwwfdeutschland'
    df_nprof.loc[df_nprof['owner_merge'].str.count(
        'freundedesdeutschpolnisch') > 0, 'owner_merge'] = 'vereinfreundedesdeutschpolnischeneuropanationalparksunteresodertalev'
    df_nprof.loc[df_nprof['owner_merge'].str.count(
        'vogelschutzkomiteeev') > 0, 'owner_merge'] = 'vskvogelschutzkomiteeevgesellschaftzur'
    df_nprof.loc[df_nprof['owner_merge'].str.count(
        'zoologischegesellschaftfra') > 0, 'owner_merge'] = 'zoologischegesellschaftfrankfurtvon1858ev'

    ## Get df with unique nonprof names and their frequencies (comes in ascending order)
    df_nprof_uni = df_nprof.groupby(['owner_merge']).size().reset_index(name='Freq').copy()

    ## Fuzzy matching
    fuzzy_match_token_set_ratio(df_nprof_uni, 'owner_merge', 'owner_new', 'Freq', 97, 20)

    ## Create dictionary with old names and new names
    own_dict = {}
    for row in df_nprof_uni.itertuples():
        owner_merge = row.owner_merge
        owner_new = row.owner_new
        own_dict[owner_merge] = owner_new

    ## Assign new names based on dictionary
    uni_owners = list(df_nprof_uni['owner_merge'].astype('str').unique())
    for uni_own in uni_owners:
        df_nprof.loc[df_nprof['owner_merge'].astype('str') == uni_own, 'owner_merge'] = own_dict[uni_own]

    ## Decide on one address based on occurences
    print(len(df_nprof))
    owner_names = df_nprof['owner_merge'].unique().tolist()

    choose_address_based_on_occurences(df=df_nprof, owner_names=owner_names, out_pth=NONPROF_OWNERS_TEMP)
    df_assigned = pd.read_csv(NONPROF_OWNERS_TEMP, sep=';', encoding='ISO-8859-1')
    cols_left = ['OGC_FID', 'EIGENTUEME', 'AMTLFLSFL', 'area', 'owner_names', 'own_num', 'owner_clean', 'category',
                 'owner_merge', 'level3', 'level2', 'level1']

    # t = df_assigned.loc[df_assigned['owner_merge'].isin(owner_names)].copy()
    df_out = pd.merge(df_nprof[cols_left], df_assigned, how='left', on='owner_merge')
    print(len(df_out))
    df_out.to_csv(NONPROF_OWNERS_PTH, sep=';', index=False)



def clean_religious_identifiers():
    df = pd.read_csv(OUT_PTH, sep=';')
    ## CLEAN ALL RELIGIOUS ENTITIES
    print('Religious')
    df_rel = df[df['level1'] == 4].copy()

    ## Aggregate manually import entities
    df_rel.loc[
        df_rel['owner_merge'].str.count('conferenceonjewish') > 0, 'owner_merge'] = 'conferenceonjewishmaterialclaimsinc'

    ## Decide on one address based on occurences
    print(len(df_rel))
    owner_names = df_rel['owner_merge'].unique().tolist()

    choose_address_based_on_occurences(df=df_rel, owner_names=owner_names, out_pth=RELIGIOUS_OWNERS_TEMP)
    df_assigned = pd.read_csv(RELIGIOUS_OWNERS_TEMP, sep=';', encoding='ISO-8859-1')
    cols_left = ['OGC_FID', 'EIGENTUEME', 'AMTLFLSFL', 'area', 'owner_names', 'own_num', 'owner_clean', 'category',
                 'owner_merge', 'level3', 'level2', 'level1']

    # t = df_assigned.loc[df_assigned['owner_merge'].isin(owner_names)].copy()
    df_out = pd.merge(df_rel[cols_left], df_assigned, how='left', on='owner_merge')
    print(len(df_out))
    df_out.to_csv(RELIGIOUS_OWNERS_PTH, sep=';', index=False)

def clean_public_identifiers():
    df = pd.read_csv(OUT_PTH, sep=';')
    ## CLEAN ALL PUBLIC ENTITIES
    print('Public')
    df_pub = df[df['level1'] == 5].copy()
    df_pub_uni = df_pub.groupby(['owner_merge']).size().reset_index(name='Freq').copy()

    ## Aggregate manually import entities
    df_pub.loc[df_pub['owner_merge'].str.count('bvvg') > 0, 'owner_merge'] = 'bvvg'
    df_pub.loc[df_pub['owner_merge'].str.count('berlinerstadtgueter') > 0, 'owner_merge'] = 'berlinerstadtguetergmbh'
    df_pub.loc[df_pub['owner_merge'].str.count('bundesstrass') > 0, 'owner_merge'] = 'bundesstrassenverwaltung'
    df_pub.loc[df_pub['owner_merge'].str.count('bundeswasserstrass') > 0, 'owner_merge'] = 'bundeswasserstrassenverwaltung'
    df_pub.loc[df_pub['owner_merge'].str.count(
        'bundesanstaltfuerimmobilien') > 0, 'owner_merge'] = 'bundesanstaltfuerimmobilienaufgaben'
    df_pub.loc[df_pub['owner_merge'].str.count(
        'bundesanstaltfuervereinigungs') > 0, 'owner_merge'] = 'bundesanstaltfuervereinigungsbedingtesonderaufgaben'
    df_pub.loc[df_pub['owner_merge'].str.count('bundesfinanz') > 0, 'owner_merge'] = 'brdbundesfinanzverwaltung'
    df_pub.loc[df_pub['owner_merge'].str.count('landberli') > 0, 'owner_merge'] = 'landberlin'
    df_pub.loc[df_pub['owner_merge'].str.count('brandenburggrund') > 0, 'owner_merge'] = 'landbrandenburggrundstuecksfond'
    # df_pub.loc[df_pub['owner_merge'].str.count('landbra') > 0, 'owner_merge'] = 'landbrandenburg'

    df_pub_uni = df_pub.groupby(['owner_merge']).size().reset_index(name='Freq').copy()
    ## Fuzzy matching
    fuzzy_match_token_set_ratio(df_pub_uni, 'owner_merge', 'owner_new', 'Freq', 97, 20)

    ## Create dictionary with old names and new names
    own_dict = {}
    for row in df_pub_uni.itertuples():
        owner_merge = row.owner_merge
        owner_new = row.owner_new
        own_dict[owner_merge] = owner_new

    ## Assign new names based on dictionary
    uni_owners = list(df_pub_uni['owner_merge'].astype('str').unique())
    for uni_own in uni_owners:
        # print(uni_own)
        df_pub.loc[df_pub['owner_merge'].astype('str') == uni_own, 'owner_merge'] = own_dict[uni_own]

    ## Decide on one address based on occurences
    print(len(df_pub))
    owner_names = df_pub['owner_merge'].unique().tolist()

    choose_address_based_on_occurences(df=df_pub, owner_names=owner_names, out_pth=PUBLIC_OWNERS_TEMP)
    df_assigned = pd.read_csv(PUBLIC_OWNERS_TEMP, sep=';', encoding='ISO-8859-1')
    cols_left = ['OGC_FID', 'EIGENTUEME', 'AMTLFLSFL', 'area', 'owner_names', 'own_num', 'owner_clean', 'category',
                 'owner_merge', 'level3', 'level2', 'level1']

    # t = df_assigned.loc[df_assigned['owner_merge'].isin(owner_names)].copy()
    df_out = pd.merge(df_pub[cols_left], df_assigned, how='left', on='owner_merge')
    print(len(df_out))

    # df_out.loc[df_out['clean_address'].isnan(), 'clean_address'] = 'seeburger chaussee 2, 14476 potsdam'
    # df_out.loc[df_out['clean_address'].isnan(), 'owner_clean'] = 'land brandenburg landesnaturschutzflaechenverwaltung'
    # df_out.loc[df_out['clean_address'].isnan(), 'owner_merge'] = 'landbrandenburglandesnaturschutzflaechenverwaltung'

    df_out.to_csv(PUBLIC_OWNERS_PTH, sep=';', index=False)


def clean_private_owners_identifiers():
    df = pd.read_csv(OUT_PTH, sep=';')
    ## CLEAN ALL PRIVATE OWNERS
    print('Private')
    df_priv = df[df['level1'] == 1].copy()
    del df

    ## Split between single owners and groups of owners
    df_done = df_priv.loc[df_priv['category'] != 1].copy()
    df_work = df_priv.loc[df_priv['category'] == 1].copy()

    df_work_uni = df_work.drop_duplicates(subset=['owner_merge', 'clean_address'])
    df_name_count = df_work_uni[['owner_merge', 'clean_address']].groupby(by='owner_merge').count().reset_index()
    df_name_count.rename(columns={'clean_address': 'address_count'}, inplace=True)
    df_mult = df_name_count.loc[df_name_count['address_count'] > 1].copy()
    df_single = df_name_count.loc[df_name_count['address_count'] <= 1].copy()

    ## in df single, there are owners with multiple addresses that always occur with these addresses
    ## separate those from the others and assign first address to them
    df_temp = df_work_uni.loc[df_work_uni['owner_merge'].isin(df_single['owner_merge'])].copy()

    df_single_ambiguos = df_temp.loc[df_temp["clean_address"].str.count('_') > 0].copy()
    df_single = df_temp.loc[df_temp["clean_address"].str.count('_') == 0].copy()

    df_assigned3 = df_work_uni.loc[df_work_uni['owner_merge'].isin(df_single_ambiguos['owner_merge'])].copy()
    df_assigned3 = df_assigned3[["owner_merge", "addresses"]]
    df_assigned3["clean_address"] = df_assigned3.apply(lambda row: row.addresses.split('_')[0], axis=1)

    ##TODO remove clean addresses with '_'
    # def most_frequent(in_list):
    #     return max(set(in_list), key=in_list.count)
    #
    # file = open(PRIVATE_OWNERS_TEMP, "w+")
    # file.write("owner_merge;addresses;clean_address\n")
    #
    # for owner in df_mult['owner_merge']:
    #     sub = df_work.loc[df_work['owner_merge'] == owner].copy()
    #     addresses = sub['clean_address'].tolist()
    #     uni_addresses = []
    #     for address in addresses:
    #         lst = address.split('_')
    #         for address in lst:
    #             if address != 'unbekannt':
    #                 uni_addresses.append(address)
    #     mf_address = most_frequent(uni_addresses)
    #     file.write(f"{owner};{'_'.join(list(set(uni_addresses)))};{mf_address}\n")
    #     # out_dict["owner_merge"].append(owner)
    #     # out_dict["addresses"].append('_'.join(list(set(uni_addresses))))
    #     # out_dict["clean_address"] = mf_address
    # file.close()

    df_assigned1 = df_work_uni.loc[df_work_uni['owner_merge'].isin(df_single['owner_merge'])].copy()
    df_assigned1 = df_assigned1[["owner_merge", "addresses", "clean_address"]]
    df_assigned2 = pd.read_csv(PRIVATE_OWNERS_TEMP, sep=';', encoding='ISO-8859-1')
    del df_work_uni, df_mult, df_single, df_name_count, df_temp

    df_assigned = pd.concat([df_assigned1, df_assigned2, df_assigned3], axis=0)
    del df_assigned1, df_assigned2, df_assigned3

    cols_left = ['OGC_FID', 'EIGENTUEME', 'AMTLFLSFL', 'area', 'owner_names', 'own_num', 'owner_clean', 'category',
                 'owner_merge', 'level3', 'level2', 'level1']
    df_work = pd.merge(df_work[cols_left], df_assigned, how='left', on='owner_merge')

    ## Reorder columns to original order
    original_order = list(df_priv.columns)
    if "Unnamed: 0" in original_order:
        original_order.remove("Unnamed: 0")
    df_work = df_work[original_order]

    df_work.rename(columns={'owner_merge': 'owner_merge0'}, inplace=True)
    df_work['owner_clean'] = df_work['owner_clean'].str.strip(',')
    df_work['owner_clean'] = df_work['owner_clean'].str.strip(' ')

    df_work['fam_name'] = df_work.apply(lambda row: row.owner_clean.split(',')[0], axis=1)
    df_work['owner_merge'] = df_work['fam_name'] + df_work['clean_address']
    df_work['owner_merge'] = df_work['owner_merge'].str.replace(' ', '', regex=False)
    df_work['owner_merge'] = df_work['owner_merge'].str.replace('-', '', regex=False)
    df_work['owner_merge'] = df_work['owner_merge'].str.replace(',', '', regex=False)
    df_work['owner_merge'] = df_work['owner_merge'].str.replace('&', '', regex=False)
    df_work['owner_merge'] = df_work['owner_merge'].str.replace('+', '', regex=False)
    df_work['owner_merge'] = df_work['owner_merge'].str.replace('`', '', regex=False)

    ## Filter by birthdate
    ############ uncomment if you want to start here ############
    ### df = pd.read_csv(OUT_PTH, sep=';')
    # df_priv = df[df['level1'] == 1].copy()
    #############################################################

    df_work['birthdate'] = df_work['owner_clean'].apply(get_birthdate)
    df_work['birthdate'] = df_work['birthdate'].str.replace(' ', '')
    df_work['birthdate'] = df_work['birthdate'].replace({'\*', ''}, regex=True)
    df_work['birthdate'] = df_work['birthdate'].str.replace('-', '')

    df_work['birthdate'] = pd.to_datetime(df_work['birthdate'], format='%Y%m%d', errors='coerce')
    date_accq = datetime.datetime(year=2020, month=11, day=15)
    # df['age'] = (now - df['dob']).astype('<m8[Y]')    # 3
    df_work['age_accq'] = (date_accq - df_work['birthdate']).astype('<m8[Y]')

    df_work.loc[df_work['age_accq'] > 101, 'owner_merge'] = 'unbekannt'
    df_work.loc[df_work['age_accq'] > 101, 'owner_clean'] = 'unbekannt'
    df_work.loc[df_work['age_accq'] > 101, 'clean_address'] = 'unbekannt'
    df_work.loc[df_work['age_accq'] > 101, 'category'] = 28
    df_work.loc[df_work['age_accq'] > 101, 'level3'] = '5_2_5'
    df_work.loc[df_work['age_accq'] > 101, 'level2'] = '5_2'
    df_work.loc[df_work['age_accq'] > 101, 'level1'] = 5

    df_work.loc[df_work['age_accq'].isna(), 'age_accq'] = -9999

    df_work.to_csv(PRIVATE_OWNERS_PTH, sep=';', index=False)
    df_done.to_csv(PRIVATE_OWNERS_GROUPS_PTH, sep=';', index=False)

    # fig = df_priv.loc[df_priv['age_accq'] <= 110, 'age_accq'].dropna().plot(kind='hist', bins=110, grid=True,
    #                                                                         xticks=range(0, 110, 5), xlabel='Age')
    # fig.savefig(r"C:\Users\IAMO\Documents\work_data\chapter1\figures\Private_owners_age_histogramm.png")

def merge_all_class_dfs():
    df = pd.read_csv(OWNERS_PTH, sep=';')
    print(len(df))
    df_priv = pd.read_csv(PRIVATE_OWNERS_PTH, sep=';')
    df_priv_groups = pd.read_csv(PRIVATE_OWNERS_GROUPS_PTH, sep=';')
    df_comp = pd.read_csv(COMPANY_OWNERS_PTH, sep=';')
    df_nprof = pd.read_csv(NONPROF_OWNERS_PTH, sep=';')
    df_rel = pd.read_csv(RELIGIOUS_OWNERS_PTH, sep=';')
    df_pub = pd.read_csv(PUBLIC_OWNERS_PTH, sep=';')

    ## Bring all columns of all data frames into the same order
    columns = df_comp.columns
    df_priv = df_priv[columns]
    df_priv_groups = df_priv_groups[columns]
    df_nprof = df_nprof[columns]
    df_rel = df_rel[columns]
    df_pub = df_pub[columns]

    ## Merge all the separate dfs
    df_out = pd.concat([df_priv, df_priv_groups, df_comp, df_nprof, df_rel, df_pub])
    df_out.sort_values(by='OGC_FID', inplace=True)
    print(len(df_out))
    df_out.to_csv(OUT_PTH, sep=";", index=False)



    # ## Get statistics for aggregated df
    # print('Stats after aggregation')
    # AREA = 'area'
    # FID = 'OGC_FID'
    # OWNERS = 'owner_merge'
    # overall_stats_wrapper(df_out, STATS_OUT_PTH, AREA, FID, OWNERS)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)

    os.chdir(WD)

    # reclassify_owner_classes()
    # clean_company_identifiers()
    # clean_non_profit_identifiers()
    # clean_religious_identifiers()
    # clean_public_identifiers()
    clean_private_owners_identifiers()
    merge_all_class_dfs()

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()
