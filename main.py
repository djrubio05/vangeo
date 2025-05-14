import pandas as pd
import numpy as np
import geopandas as gpd
from math import radians, cos, sin, asin, sqrt
from geojson import FeatureCollection, Feature, LineString, Point
import random
import json


def split_valid_df(raw_df: pd.DataFrame) -> dict[pd.DataFrame]:
    lat_filter = (raw_df['lat']<90) & (raw_df['lat']>-90)
    lon_filter = (raw_df['lon']<180) & (raw_df['lon']>-180)
    timestamp_filter = (np.isnan(raw_df['timestamp']))

    invalid = raw_df.loc[~(lat_filter & lon_filter & ~timestamp_filter)]
    invalid = invalid.reset_index(drop=True)

    df = raw_df.loc[lat_filter & lon_filter & ~timestamp_filter]
    df = df.sort_values(by=['timestamp']).reset_index(drop=True)

    return {'valid' : df, 'invalid' : invalid}

def split_by_trip(gdf: pd.DataFrame) -> dict[pd.DataFrame]:
    trip = 0

    for index, row in gdf.iterrows():
        if row['distance'] > 2 or row['timedelta'] > 25:
            gdf.loc[index, 'distance'] = 0
            gdf.loc[index, 'timedelta'] = 0
            trip += 1
        gdf.loc[index,'trip'] = trip
        
    trips = list(gdf['trip'].unique())
    trips_gdfs = {}

    for trip in trips:
        trip_name = f'trip_{str(trip)}'
        per_trip_gdf = gdf.loc[gdf['trip'] == trip].reset_index(drop=True)
        per_trip_gdf = per_trip_gdf.drop('trip', axis=1)

        trips_gdfs[trip_name] = per_trip_gdf

    return trips_gdfs

def get_trip_summary(trip_gdf: gpd.GeoDataFrame) -> dict:
    trip_gdf = trips_gdfs[trip]

    speed = trip_gdf['distance'].divide(trip_gdf['timedelta']) * 60

    max_speed = speed.max()
    total_km = trip_gdf['distance'].sum()
    total_min = trip_gdf['timedelta'].sum()
    average_speed = total_km/(total_min/60)

    return {
        'distance_km' : float(total_km),
        'duration_min': float(total_min),
        'avg_speed_kmh': float(average_speed),
        'max_speed_kmh': float(max_speed)
        }

def create_feature_from_trip(trip_gdf: gpd.GeoDataFrame, line_color: str) -> Feature:
    combine = list(zip(list(trip_gdf['lon']), list(trip_gdf['lat'])))
    feature = Feature(geometry=LineString(combine), properties={'stroke' : line_color})
    return feature

def haversine(point1: Point, point2: Point) -> float:
    lon1, lat1, lon2, lat2 = map(radians, [point1.x, point1.y, point2.x, point2.y])

    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371
    return c * r

if __name__ == '__main__':

    raw_df = pd.read_csv("Project Points.csv")

    raw_df['timestamp'] = pd.to_datetime(raw_df['timestamp'], errors='coerce')

    dfs = split_valid_df(raw_df)
    df = dfs['valid']
    removed_df = dfs['invalid']

    removed_df.to_csv('rejects.log')

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs='EPSG:4326')
    gdf2 = gdf.shift()

    distance = gdf['geometry'].iloc[1:].combine(gdf2['geometry'].iloc[1:], haversine)
    gdf['distance']=distance
    gdf['distance'] = gdf['distance'].fillna(0)

    timespan = gdf['timestamp'].iloc[1:].sub(gdf2['timestamp'].iloc[1:])
    gdf['timedelta'] = timespan.apply(lambda x: x.total_seconds()/60)
    gdf['timedelta'] = gdf['timedelta'].fillna(0)

    trips_gdfs = split_by_trip(gdf)
        
    trips_jsons = {}
    feature_list = []
    line_colors = []
    for trip, trip_gdf in trips_gdfs.items():
        trip_gdf.to_csv(trip, index=False)
        trips_jsons[trip] = get_trip_summary(trip)

        random_color = ""
        while random_color not in line_colors:
            r = lambda: random.randint(0,255)
            random_color = '#%02X%02X%02X' % (r(),r(),r())
            line_colors.append(random_color)
        
        feature = create_feature_from_trip(trip_gdf, random_color)
        feature_list.append(feature)

    feature_collection = FeatureCollection(feature_list)
    with open('trips.geojson', 'w') as f:
        json.dump(feature_collection, f)
