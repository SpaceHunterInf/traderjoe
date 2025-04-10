import pandas as pd
import geopandas as gpd
import os
import matplotlib.pyplot as plt
import numpy as np
import contextily as ctx
from matplotlib.colors import LinearSegmentedColormap
from shapely import wkt

raw_features_path = 'cleaned_data/merged_nyc_raw_features.geojson'
subway_data_path = 'data/subway-stations.geojson'
parking_data_path = 'data/DPR_ParkingLots_001_20250410.csv'
truck_routes_data_path = 'data/New_York_City_Truck_Routes_20250410.csv'
crime_data_path = 'data/grandlarceny.geojson'

def min_max_normalize(series):
    return (series - series.min()) / (series.max() - series.min())

nyc_data = gpd.read_file('cleaned_data/merged_nyc_raw_features.geojson')

# Load the subway station data

subway_data = gpd.read_file(subway_data_path)

# Clean up subway data - remove stations with invalid geometries
subway_clean = subway_data[~subway_data.geometry.isna()]

# Calculate area in square kilometers for each ZIP code
nyc_data['area_sqkm'] = nyc_data.to_crs(epsg=32118).area / 1000000

# Count subway stations in each ZIP code
nyc_data['subway_count'] = 0
for idx, zip_area in nyc_data.iterrows():
    stations_in_area = subway_clean[subway_clean.intersects(zip_area.geometry)]
    nyc_data.at[idx, 'subway_count'] = len(stations_in_area)

# Calculate and normalize subway station density
nyc_data['subway_density'] = nyc_data['subway_count'] / nyc_data['area_sqkm']
#nyc_data['subway_density_normalized'] = min_max_normalize(nyc_data['subway_density'])

parking_lots_df = pd.read_csv(parking_data_path)
# Convert WKT geometry to shapely objects
parking_lots_df['geometry'] = parking_lots_df['the_geom'].apply(wkt.loads)
# Create GeoDataFrame
parking_lots_gdf = gpd.GeoDataFrame(parking_lots_df, geometry='geometry', crs="EPSG:4326")

truck_routes_df = pd.read_csv(truck_routes_data_path)
# Convert WKT geometry to shapely objects
truck_routes_df['geometry'] = truck_routes_df['the_geom'].apply(wkt.loads)
# Create GeoDataFrame
truck_routes_gdf = gpd.GeoDataFrame(truck_routes_df, geometry='geometry', crs="EPSG:4326")
print(f"Found {len(truck_routes_gdf)} truck routes in NYC")

# Count parking lots in each ZIP code
print("Calculating parking lot availability by ZIP code...")
nyc_data['parking_lot_count'] = 0
for idx, zip_area in nyc_data.iterrows():
    parking_in_area = parking_lots_gdf[parking_lots_gdf.intersects(zip_area.geometry)]
    nyc_data.at[idx, 'parking_lot_count'] = len(parking_in_area)

# Calculate parking lot density (per sq km)
nyc_data['parking_density'] = nyc_data['parking_lot_count'] / nyc_data['area_sqkm']
#nyc_data['parking_density_normalized'] = min_max_normalize(nyc_data['parking_density'])

# Calculate distance to nearest truck route for each ZIP code
print("Calculating proximity to truck routes...")
nyc_data['distance_to_truck_route'] = float('inf')
for idx, zip_area in nyc_data.iterrows():
    if idx % 10 == 0:  # Progress tracking
        print(f"Processing ZIP code {idx}/{len(nyc_data)}", end='\r')
    
    # Use centroid of ZIP code area
    centroid = zip_area.geometry.centroid
    
    # Find minimum distance to any truck route
    min_distance = float('inf')
    for _, route in truck_routes_gdf.iterrows():
        distance = centroid.distance(route.geometry)
        if distance < min_distance:
            min_distance = distance
    
    nyc_data.at[idx, 'distance_to_truck_route'] = min_distance


crime_data = gpd.read_file('data/grandlarceny.geojson')

# Make sure both datasets have the same CRS
if crime_data.crs != nyc_data.crs:
    crime_data = crime_data.to_crs(nyc_data.crs)

# Count crimes in each ZIP code area
nyc_data['crime_count'] = 0
for idx, zip_area in nyc_data.iterrows():
    crimes_in_area = crime_data[crime_data.intersects(zip_area.geometry)]
    nyc_data.at[idx, 'crime_count'] = len(crimes_in_area)

# Calculate crime density (crimes per sq km)
nyc_data['crime_density'] = nyc_data['crime_count'] / nyc_data['area_sqkm']

# Normalize crime density (inverse, since lower is better)
max_crime = nyc_data['crime_density'].max()
nyc_data['crime_density_inverse'] = max_crime - nyc_data['crime_density']
# nyc_data['crime_normalized'] = (nyc_data['crime_density_inverse'] - nyc_data['crime_density_inverse'].min()) / (nyc_data['crime_density_inverse'].max() - nyc_data['crime_density_inverse'].min())

# Normalize truck route distance (inverse, since closer is better)
max_dist = nyc_data['distance_to_truck_route'].replace([np.inf], np.nan).max()
nyc_data['distance_to_truck_route_inverse'] = max_dist - nyc_data['distance_to_truck_route']
# nyc_data['truck_route_normalized'] = min_max_normalize(nyc_data['distance_to_truck_route_inverse'])

# Also normalize the rent and home value data (inverse)
nyc_data['rent_inverse'] = nyc_data['Median_Gross_Rent'].max() - nyc_data['Median_Gross_Rent']
nyc_data['home_value_inverse'] = nyc_data['Median_Home_Value'].max() - nyc_data['Median_Home_Value']
# nyc_data['rent_inverse_normalized'] = min_max_normalize(nyc_data['rent_inverse'])
# nyc_data['home_value_inverse_normalized'] = min_max_normalize(nyc_data['home_value_inverse'])

# Filter for income > 120K
#high_income_areas = nyc_data[nyc_data['Median_Household_Income'] > 120000].copy()
#print(f"Number of high income ZIP areas (>$120K): {len(high_income_areas)}")

# Normalize population data
# high_income_areas['Population_Normalized'] = min_max_normalize(high_income_areas['Total_Population'])
# high_income_areas['Young_Adult_Normalized'] = min_max_normalize(high_income_areas['Percent_25_to_44'])
# high_income_areas['White_Pop_Normalized'] = min_max_normalize(high_income_areas['Percent_White'])
# high_income_areas['Asian_Pop_Normalized'] = min_max_normalize(high_income_areas['Percent_Asian'])
# high_income_areas['Bachelor_Degree_Normalized'] = min_max_normalize(high_income_areas['Bachelors_Degree_or_Higher']/ high_income_areas['Total_Population'])
# high_income_areas['Income_Normalized'] = min_max_normalize(high_income_areas['Median_Household_Income'])

print(nyc_data.head())
# Save the cleaned and processed data to a new file
output_path = 'cleaned_data/nyc_ZCTA_raw_features.geojson'
nyc_data.to_file(output_path, driver='GeoJSON')