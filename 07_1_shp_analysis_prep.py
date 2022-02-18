# Clemens Jänicke
# github Repo: https://github.com/clejae

# ------------------------------------------ LOAD PACKAGES ---------------------------------------------------#
import os
import time
import pandas as pd
import geopandas as gpd
import json


# ------------------------------------------ USER INPUT ------------------------------------------------#
WD = r'C:\Users\IAMO\Documents\work_data\chapter1\ALKIS\\'

PARCELS_PTH = r"06_owner_class_aggregation\06_owners_aggregated.shp"
PARCELS_PTH = r"01_clean_owner_strings\v_eigentuemer_bb_reduced_25832.shp"
PTH_IACS = r"07_analysis\IACS_2020_25832.shp"

INTERSECTION_OUT_PTH = r"07_analysis\07_alkis_iacs_intersection.shp"
INTERSECTION_SLIVERS_PTH = r"07_analysis\07_alkis_iacs_intersection_slivers.shp"
SYMMETRICDIFF_OUT_PTH = r"07_analysis\07_alkis_iacs_symmetricdiff.sh"
# ------------------------------------------ LOAD DATA & PROCESSING ------------------------------------------#


def intersect_alkis_iacs():
    ## Read input
    print(f"Read {PARCELS_PTH}")
    gdf_alk = gpd.read_file(PARCELS_PTH)
    print(f"Read {PTH_IACS}")
    gdf_iacs = gpd.read_file(PTH_IACS)

    ## Reproject IACS to ALKIS if necessary
    gdf_alk_epsg = int(gdf_alk.crs.srs.split(':')[1])
    gdf_iacs_epsg = int(gdf_iacs.crs.srs.split(':')[1])
    if gdf_alk_epsg != gdf_iacs_epsg:
        print(f'Input shapefiles do not have the same projection. Reproject shp2 to epsg:{gdf_alk_epsg}')
        try:
            gdf_iacs = gdf_iacs.to_crs(gdf_alk_epsg)
        except:
            print(f'Reprojection from {gdf_alk_epsg} to {gdf_iacs_epsg} failed!')
            return

    gdf_alk = gdf_alk[['OGC_FID', 'geometry']]

    ## Subset IACS to necessary information
    gdf_iacs = gdf_iacs[['BTNR', 'CODE', 'CODE_BEZ', 'geometry']]
    gdf_iacs['OBJECTID'] = range(len(gdf_iacs))

    ## Intersect both shapefiles
    print("Intersect")
    gdf = gpd.overlay(gdf_iacs, gdf_alk, how='intersection')

    ## Calculate shape parameters
    gdf['area'] = gdf['geometry'].area
    gdf['perimeter'] = gdf['geometry'].length
    gdf['thin_ratio'] = 4 * 3.14 * (gdf['area'] / (gdf['perimeter'] * gdf['perimeter']))

    ## Clean sliver polygons
    # ## ToDo: verify somehow the threshold for the thinness ratio
    gdf_slivers = gdf.loc[gdf['thin_ratio'] <= 0.01]
    gdf_slivers.to_file(INTERSECTION_SLIVERS_PTH)

    gdf = gdf.loc[gdf['thin_ratio'] > 0.01]
    gdf = gdf.loc[gdf['area'] > 0.0]

    ## Write to disct
    print(f"Write intersection to disc {INTERSECTION_OUT_PTH}")
    gdf.to_file(INTERSECTION_OUT_PTH)

    ## Symmetric difference of both shapefiles
    print("Symmetric difference")
    gdf = gpd.overlay(gdf_iacs, gdf_alk, how='symmetric_difference')

    ## Calculate shape parameters
    gdf['area'] = gdf['geometry'].area
    gdf['perimeter'] = gdf['geometry'].length
    gdf['thin_ratio'] = 4 * 3.14 * (gdf['area'] / (gdf['perimeter'] * gdf['perimeter']))

    gdf = gdf.loc[gdf['thin_ratio'] > 0.01]
    gdf = gdf.loc[gdf['area'] > 0.0]

    ## Write to disct
    print(f"Write intersection to disc {INTERSECTION_OUT_PTH}")
    gdf.to_file(SYMMETRICDIFF_OUT_PTH)

def main():
    stime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    os.chdir(WD)


    intersect_alkis_iacs()


    etime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print("start: " + stime)
    print("end: " + etime)

if __name__ == '__main__':
    main()