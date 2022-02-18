# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import time
import pandas as pd
import json

WD = r'C:\Users\IAMO\Documents\work_data\chapter1\ALKIS\\'

OWNERS_PTH = r'02_identify_unique_owners_adress_comb\02_owners_and_addresses_stretched.csv' # r'unique_owners.csv'
CLASS_IDS_PTH = r'class_ids_l3_preliminary.json'

OUT_CLASSIFIED = r"03_owner_name_classification\03_owners_stretched_preliminary_classication.csv"
OUT_STATS = r"03_owner_name_classification\03_owners_stretched_preliminary_classication_stats.csv"
# ------------------------------------------ DEFINE FUNCTIONS ------------------------------------------------#
def replace_characters_in_string(text, char_lst, replace_string):
    """
    Replaces all characters or words from an input list with a specified string.

    :param text: Input text. String.
    :param char_lst: List of characters or words. List of strings.
    :param replace_string: The replacement string. String.
    :return:
    """
    ## check if string is text
    if type(text) != str:
        text = str(text)

    for char in char_lst:
        text = text.replace(char, replace_string)

    return text


def check_occ_of_words1(text, search_terms, return_code):
    """
    Checks if any of the search terms from the input list occurs in the text. Only returns a true (1), if an entire
    word from the text matches any search term, i.e. if a sub part of a word fits the search terms the return will be
    false (0). Example: If the search term is "py" and the text is "python", there will be no match. Only "python" will
    be matched with "python".

    :param text: Input text. String.
    :param search_terms: List of word that should be looked for. List of strings.
    :return: Boolean integer. 1: there is a match, 0: there is no match.
    """

    check = 0

    if text != None:
        text = text.replace(',', ' ')
        str_lst = text.split(" ")
        for search_term in search_terms:
            if search_term in str_lst:
                check = return_code
                break
            else:
                pass
    else:
        pass

    return check


def check_occ_of_words2(text, search_terms, return_code):
    """
    Checks if any of the search terms from the input list occurs in the text, also considers sub parts of words.
    Example: If the search term is "py" and the text is "python", there will be a match.

    :param text: Input text. String.
    :param search_terms: List of word that should be looked for. List of strings.
    :return: Boolean integer. 1: there is a match, 0: there is no match.
    """

    check = 0
    if text != None:
        for search_term in search_terms:
            if search_term in text:
                check = return_code
                break
            else:
                pass
    else:
        pass

    return check


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


def classify_owner_names_into_classes():
    ## Read input
    df = pd.read_csv(OWNERS_PTH, sep=';')
    with open(CLASS_IDS_PTH) as json_file:
        class_ids = json.load(json_file)

    ## Clean owner string
    df['owner_clean'] = df['owner_names'].str.lower()

    ## Remove strings
    # Don't remove '&' and '\+', because they are needed to identify mixed legal forms (& co ..)
    df['owner_clean'] = df['owner_clean'].apply(
        replace_characters_in_string,
        char_lst= ['.', ';', r'\\', '/', ':', '"', "'", '(', ')', '\+', '?'],
        replace_string='')

    df['owner_clean'] = df['owner_clean'].str.replace('ä', 'ae')
    df['owner_clean'] = df['owner_clean'].str.replace('ö', 'oe')
    df['owner_clean'] = df['owner_clean'].str.replace('ü', 'ue')
    df['owner_clean'] = df['owner_clean'].str.replace('ß', 'ss')

    ## Replace common word chains with typical abbreviations
    df['owner_clean'] = df['owner_clean'].str.replace('gesellschaft buergerlichen rechts', 'gbr')
    df['owner_clean'] = df['owner_clean'].str.replace('gesellschaft mit beschraenkter haftung', 'gmbh')
    df['owner_clean'] = df['owner_clean'].str.replace('mit beschraenkter haftung', 'mbh')
    df['owner_clean'] = df['owner_clean'].str.replace('kommanditgesellschaft', 'kg')
    df['owner_clean'] = df['owner_clean'].str.replace('kommandit-gesellschaft', 'kg')
    df['owner_clean'] = df['owner_clean'].str.replace('eingetragene genossenschaft', 'eg')
    df['owner_clean'] = df['owner_clean'].str.replace('aktiengesellschaft', 'ag')
    df['owner_clean'] = df['owner_clean'].str.replace('deutsche bahn', 'db')
    df['owner_clean'] = df['owner_clean'].str.replace('bundesrepublik', 'brd')
    df['owner_clean'] = df['owner_clean'].str.replace('eigentum des volkes', 'edv')
    df['owner_clean'] = df['owner_clean'].str.replace('gemeinnuetzige gmbh', 'ggmbh')
    df['owner_clean'] = df['owner_clean'].apply(remove_substring_and_followup, substring='mit dem sitz in')
    df['owner_clean'] = df['owner_clean'].apply(remove_substring_and_followup, substring='mit sitz in')
    df['owner_clean'] = df['owner_clean'].apply(remove_substring_and_followup, substring=', sitz')

    address_code_words = ['straße', 'strasse', 'weg', ' zum ', 'dorf', 'ausbau', 'chausee', ' ot ', ' am ', ' an ', 'str ']
    df['owner_clean'] = df['owner_clean'].apply(remove_address_part, delimiter=',',
                                                address_code_words=address_code_words)

    df.loc[df['owner_clean'].isna(), 'owner_clean'] = 'unbekannt'

    ## Classify all private persons based on asterisk
    df['category'] = 0
    df.loc[df['owner_clean'].str.count('\*') == 1, 'category'] = 1  # private person
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())

    ## Classify different "Gesellschaftsformen" with help of code words
    # All mixed legal forms
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['& co', ' &co', '& c o', '6 co', '+ co', '& go', 'und co', ' u co'],
        return_code=class_ids['mixed'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    ## gemeinnützige
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['ggmbh', 'gemeinnuetzige'],
        return_code=class_ids['gemeinn'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # BVVG
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['bvvg', 'treuhandanstalt', 'bodenverwert'],
        return_code=class_ids['bvvg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # GbR
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['gbr'],
        return_code=class_ids['gbr'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # OHG
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['ohg'],
        return_code=class_ids['ohg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # KG
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['kg', 'kommanditgesellschaft'],
        return_code=class_ids['kg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # EWIV
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['ewiv'],
        return_code=class_ids['ewiv'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # AG
    df.loc[df['category'] == 0, 'category']=df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['ag'],
        return_code=class_ids['ag'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['aktiengesellschaft', 'agraraktiengesellschaft'],
        return_code=class_ids['ag'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # GmbH
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['gmbh', 'mbh', 'mit beschraenkter haf'],
        return_code=class_ids['gmbh'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # ug
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['ug'],
        return_code=class_ids['ug'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['ug haftungsbeschraenkt', 'unternehmergesellschaft'],
        return_code=class_ids['ug'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # SE
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['se'],
        return_code=class_ids['se'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # limited
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['limited', 'sa', 'sarl', 'sárl', 'sàrl', 'holding', 'ltd'],
        return_code=class_ids['lim'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # eG
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['eg'],
        return_code=class_ids['eg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=[' e g'],
        return_code=class_ids['eg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Kirche
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2,
        search_terms=['christlich', 'evangelisch', 'katholisch', 'kirche', 'diakonie', 'pfarr', 'pfarrgemeinde', 'abtei',
                        'pfarrstelle', 'diakonisch',  'diaconat', 'diakonat', 'domstift', 'kantorat', 'predigerstelle',
                        'stift zum', 'juedisch', 'kirchgemeinde', 'hospital', 'jewish'],
        return_code=class_ids['kirch'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # e.V.
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['ev', 'verein'],
        return_code=class_ids['ev'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    df.loc[df['category'] == 0 , 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=[' e v', 'nabu', 'naturschutzbund'],
        return_code=class_ids['ev'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # w.V.
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['wv'],
        return_code=class_ids['wv'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Stiftungen
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['stiftung','naturschutzfonds','wwf'],
        return_code=class_ids['stift'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Nicht mehr existierende Institutionen
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['edv','rt', 'lpg'],
        return_code=class_ids['verg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['eigentum des volkes', 'des volk', 'separation', 'volkseigentum', 'rezess'],
        return_code=class_ids['verg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Unbekannt
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['unbekannt', 'nicht ermittelt', 'separation', 'aufgegeben', 'verstorben',
                                             'herrenlos', 'ermittel', 'nicht erfasst', 'seperation'],
        return_code=class_ids['unbek'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Gemeinden
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['gemeinde'],
        return_code=class_ids['gem'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2,
        search_terms=['stadtgemeinde', 'dorfgemeinde', 'landgemeinde', 'gemeindemitglieder', 'geimeindeverwaltung',
                        'anlieger', 'anliegenden', 'angrenzenden', 'oeffentlich'],
        return_code=class_ids['gem'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Land
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['land brandenburg', 'land brandenbung','landkreis', 'land berlin',
                                             'freistaat bayern', 'land hessen', 'land niedersachsen', 'freistaat thueringen',
                                             'landesstrassenverwaltung', 'landesbetrieb', 'landesregierung',
                                             'landesvermessung', 'nordrhein-westfalen', 'land baden',
                                             'land sachsen-anhalt'],
        return_code=class_ids['land'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Bund
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['bundesrepublik', 'bundesanstalt', 'bundesstrassenverwaltung', 'brd',
                                             'bundesfinanzverwaltung', 'bundesministerium', 'bunderepublik',],
        return_code=class_ids['bund'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Zweckverband
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['zweckverband', 'wasserverband', 'verband'],
        return_code=class_ids['zweck'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['verband'],
        return_code=class_ids['zweck'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Erbengemeinschaften
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['erbengemein'],
        return_code=class_ids['erben'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())

    ## Second round, now confusion is less likely and check_occ_of_words2 can be applied for all
    # GbR
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['bgb', 'gbr', 'gesellschaft buergerlichen r','landwirtschaftsbetrieb'],
        return_code=class_ids['gbr'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Gemeinde
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['stadt','amt'],
        return_code=class_ids['gem'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # EG
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['agrargenossenschaft', 'produktions', 'bauerngenossenschaft',
                                             'weidegenossenschaft', 'waldgenossenschaft', 'fischergenossenschaft',
                                             'gaertnergenossenschaft', 'huefnergenossenschaft', 'ackerbuergergenossenschaft'],
        return_code=class_ids['eg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Vereine
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['verein','club'],
        return_code=class_ids['ev'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # limited
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['bv','holding'],
        return_code=class_ids['lim'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Zweckverband
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['verband', 'wohnungseigentuemergemeinschaft', 'interessengemeinschaft',
                                             'teilnehmergemeinschaft', 'guetergemeinschaft'],
        return_code=class_ids['zweck'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # OHG
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['ohg'],
        return_code=class_ids['ohg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Nicht mehr existierende Institutionen
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words1, search_terms=['ddr'],
        return_code=class_ids['verg'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())

    # Privatpersonen
    # All remaining owner names with a comma should be private owners, which don't have a birthdate
    df.loc[(df['category'] == 0) & (df['owner_clean'].str.count(',') > 0), 'category'] = class_ids['priv']
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # Unbekannt
    # All remaining owner names that have less than 10 characters are likely not identifiable
    df.loc[(df['category'] == 0) & (df['owner_clean'].str.len() < 10), 'category'] = class_ids['unbek']
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # All the rest will go into unbekannt
    df.loc[(df['category'] == 0), 'category'] = class_ids['unbek']
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())

    # Mixed forms of 'Kapitalgesellschaften'
    # gmbh & co kg
    df.loc[(df['category'] == class_ids['mixed']) & (df['owner_clean'].str.count('mbh') > 0) &
           (df['owner_clean'].str.count('co') > 0) & (df['owner_clean'].str.count('kg') > 0),
            'category'] = class_ids['gmco']
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # The following command sets all mixed cases that do not match to zero (important to keep in mind for next steps)
    df.loc[df['category'] == class_ids['mixed'], 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['mbh & co kg', 'mbh & cokg', 'mbh & coko', 'mbh & co ko', 'gmbh u cokg',
                                             'gmbh + co kg', 'mbh & co', 'haftung & co', 'gmbh und co', 'gmbh &co'],
        return_code=class_ids['gmco'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # ag & co kg
    df.loc[(df['category'] == 0) & (df['owner_clean'].str.count('ag') > 0) &
           (df['owner_clean'].str.count('co') > 0) & (df['owner_clean'].str.count('kg') > 0),
            'category'] = class_ids['agco']
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['ag & co kg', 'ag & go kg'],
        return_code=class_ids['agco'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # ug & co kg
    df.loc[(df['category'] == 0) & (df['owner_clean'].str.count('ug') > 0) &
            (df['owner_clean'].str.count('co') > 0) & (df['owner_clean'].str.count('kg') > 0),
            'category'] = class_ids['ugco']
    df.loc[df['category'] == 0, 'category'] = df['owner_clean'].apply(
        check_occ_of_words2, search_terms=['ug haftungsbeschraenkt & co kg', 'ughaftungsbeschraenkt & co kg', 'ug & co kg',
                                             'ug haftungebeschraenkt & cokg', 'ug & cokg'],
        return_code=class_ids['ugco'])
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())
    # other mixes
    df.loc[(df['category'] == 0), 'category'] = class_ids['andco']
    print(len(df.loc[df['category'] != 0].copy()), df['category'].unique())

    ## Manual assignments
    df.loc[df['owner_clean'].str.count('berliner stadtgueter') > 0, 'category'] = class_ids['land']

    ## Write output
    df.to_csv(OUT_CLASSIFIED, sep=';', index=False)

def calculate_statistics():
    df = pd.read_csv(OUT_CLASSIFIED, sep=';')

    with open(CLASS_IDS_PTH) as json_file:
        class_ids = json.load(json_file)

    ## Descriptive statistics
    df_owners = df.drop_duplicates(subset='owner_clean')
    df_count = df_owners[['owner_clean', 'category']].groupby('category').count().reset_index()
    df['p_count'] = 1
    df_pcount = df[['p_count', 'category']].groupby('category').sum().reset_index()
    df_area = df[['area', 'category']].groupby('category').sum().reset_index()
    df_area['area'] = df_area['area']/10000

    df_stats = pd.DataFrame([[k, v] for k, v in class_ids.items()], columns=['class_name', 'class_id'])
    df_stats = pd.merge(df_stats, df_count, left_on='class_id', right_on='category', how='left').drop(columns='category')
    df_stats = pd.merge(df_stats, df_pcount, left_on='class_id', right_on='category', how='left').drop(columns='category')
    df_stats = pd.merge(df_stats, df_area, left_on='class_id', right_on='category', how='left').drop(columns='category')

    df_stats.columns = ['class_name', 'class_id', 'number of owners', 'number of parcels', 'area [ha]']
    df_stats.to_csv(OUT_STATS, sep=';', index=False)


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)

    os.chdir(WD)

    classify_owner_names_into_classes()
    calculate_statistics()

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()
