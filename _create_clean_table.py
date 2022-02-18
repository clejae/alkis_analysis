import pandas as pd
import os

OWNER_CAT_PTH = r"05_calculate_geolocation\05_owners_stretched_addresses_geolocated_iacs_dist.csv"
WD = r'C:\Users\IAMO\Documents\work_data\chapter1\tables\ALKIS\\'

os.chdir(WD)
df = pd.read_csv(OWNER_CAT_PTH, sep=';')

