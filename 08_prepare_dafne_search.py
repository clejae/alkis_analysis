import pandas as pd
import math
import glob
import os
import time

WD = r"C:\Users\IAMO\Documents\work_data\chapter1\ALKIS"

IN_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified_private-companies.csv"
IN_PTH = r"04_owner_class_reclassification\04_owners_stretched_classified_non-profit-etc.csv"
# MATCHES_FOLDER = r"04_owner_class_reclassification"
OUT_FOLDER = r"08_prepare_dafne_search"

NAME_COL = 'owner_clean'


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

def prepare_lists():

    ## Read list of companies
    df1 = pd.read_csv(IN_PTH, sep=';')
    df1 = df1[[NAME_COL]]
    df1.drop_duplicates(subset=NAME_COL, inplace=True)

    ## Read already matched companies
    lst = glob.glob(rf"{WD}\{OUT_FOLDER}\*matches_v1.xlsx")
    df_lst = [pd.read_excel(pth) for pth in lst]
    df_done = pd.concat(df_lst, axis=0)

    ## Clean names of companies
    clean_name_col = 'clean_name'

    df1[clean_name_col] = df1[NAME_COL].apply(remove_substring_and_followup, substring='mit dem sitz in')
    df1[clean_name_col] = df1[clean_name_col].apply(remove_substring_and_followup, substring='mit sitz in')
    df1[clean_name_col] = df1[clean_name_col].apply(remove_substring_and_followup, substring=', sitz')

    address_code_words = ['straße', 'strasse', 'weg', ' zum ', 'dorf', 'ausbau', 'chausee', ' ot ', ' am ', ' an ']
    df1[clean_name_col] = df1[clean_name_col].apply(remove_address_part, delimiter=',', address_code_words=address_code_words)

    df1.drop_duplicates(subset=clean_name_col, inplace=True)
    df1 = df1.loc[~df1[clean_name_col].isin(df_done['Unternehmensname'])].copy()

    x = int(lst[-1].split('_')[-3])
    x = 9

    num_rows = len(df1)
    num_lists = math.ceil(num_rows / 1000)

    s = 0
    for i in range(1, num_lists + 1):
        e = i * 1000
        df_out = df1[s:e]
        s += 1000
        # out_pth = rf"{WD}\{OUT_FOLDER}\batch_search_companies_{i+x:02d}.txt"
        out_pth = rf"{WD}\{OUT_FOLDER}\batch_search_non_profit_{i + x:02d}.txt"
        df_out[[clean_name_col]].to_csv(out_pth, sep=';', header=None, index=None)

def prepare_delivery():

    ## Read already matched companies
    lst = glob.glob(rf"{WD}\{OUT_FOLDER}\*matches_v1.xlsx")
    df_lst = [pd.read_excel(pth) for pth in lst]
    for i, df in enumerate(df_lst):
        df["ID"] = i
    df_done = pd.concat(df_lst, axis=0)
    df_done.loc[df_done["ID"] < 8, "type"] = "company"
    df_done.loc[df_done["ID"] >= 8, "type"] = "vereine, stiftungen, non-profit, etc"
    df_done.drop(columns=["ID"], inplace=True)

    df_done.drop_duplicates(subset="Unternehmensname", inplace=True)

    df_matches = df_done.loc[df_done['Gematchte BvD ID'].notna()].copy()
    df_misses = df_done.loc[df_done['Gematchte BvD ID'].isna()].copy()

    df_misses.reset_index(inplace=True)
    df_matches.reset_index(inplace=True)
    df_misses.drop(columns="index", inplace=True)
    df_matches.drop(columns="index", inplace=True)

    len(df_done.loc[df_done['type']=='company'])

    out_pth = r"C:\Users\IAMO\Documents\work_data\chapter1\ALKIS\08_prepare_dafne_search\Unternehmen.xlsx"
    with pd.ExcelWriter(out_pth) as writer:
        df_matches.to_excel(writer, sheet_name="Matches")
        df_misses.to_excel(writer, sheet_name="Misses")


def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)

    os.chdir(WD)

    # prepare_lists()
    prepare_delivery()

    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()