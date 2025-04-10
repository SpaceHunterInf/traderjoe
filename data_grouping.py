# This script gathers and cleans data from various sources, including income, demographics, education, and housing characteristics in New York City.

import pandas as pd
import geopandas as gpd
import os
import matplotlib.pyplot as plt
import numpy as np

nyc_map_path = 'data/MODZCTA_20250409.geojson'
nyc_income_path = 'data/ACSST5Y2023.S1903_2025-04-09T104535/ACSST5Y2023.S1903-Data.csv'
nyc_demographics_path = 'data/ACSDP5Y2023.DP05_2025-04-09T124250/ACSDP5Y2023.DP05-Data.csv'
nyc_property_value_path = 'data/ACSDP5Y2023.DP04_2025-04-09T161430/ACSDP5Y2023.DP04-Data.csv'
nyc_education_path = 'data/ACSDP5Y2023.DP02_2025-04-09T124018/ACSDP5Y2023.DP02-Data.csv'

gdf = gpd.read_file(nyc_map_path)
income_data = pd.read_csv(nyc_income_path)
income_data = income_data[['NAME', 'S1903_C03_015E']]
income_data = income_data.rename(columns={
    'NAME': 'ZCTA', 
    'S1903_C03_015E': 'Median_Household_Income'
})
income_data = income_data.iloc[1:]
#print(income_data.head())

income_data['ZCTA'] = income_data['ZCTA'].str.extract(r'ZCTA5\s(\d+)')
income_data['Median_Household_Income'] = income_data['Median_Household_Income'].replace('250,000+', '250000')
income_data['Median_Household_Income'] = pd.to_numeric(income_data['Median_Household_Income'], errors='coerce')

#print(income_data.head())
gdf['ZCTA'] = gdf['modzcta'].astype(str)
# Merge geospatial data with income data
merged_gdf = gdf.merge(income_data, on='ZCTA', how='left')

education_data = pd.read_csv(nyc_education_path)
# Select relevant columns from social data
edu_data = education_data[['NAME', 'DP02_0068E']]
# Rename columns for better readability
edu_data = edu_data.rename(columns={
    'NAME': 'ZCTA',
    'DP02_0068E' : 'Bachelors_Degree_or_Higher',
})
edu_data = edu_data.iloc[1:]

# Clean edu_df - extract just the ZIP code from the ZCTA name and convert education numbers to numeric
edu_data['ZCTA'] = edu_data['ZCTA'].str.extract(r'ZCTA5\s(\d+)')
edu_data['Bachelors_Degree_or_Higher'] = pd.to_numeric(edu_data['Bachelors_Degree_or_Higher'], errors='coerce')

# Merge geospatial data with education data
merged_gdf = merged_gdf.merge(edu_data, on='ZCTA', how='left')

# Read the demographic data file
demographic_data = pd.read_csv(nyc_demographics_path)

# Select and extract relevant columns
demo_data = demographic_data[['NAME', 'DP05_0001E', 'DP05_0010E', 'DP05_0011E', 'DP05_0037E', 'DP05_0047E']]

# Skip the first row which contains column descriptions
demo_data = demo_data.iloc[1:]

# Rename columns for better readability
demo_data = demo_data.rename(columns={
    'NAME': 'ZCTA',
    'DP05_0001E': 'Total_Population',
    'DP05_0010E': 'Population_25_to_34',
    'DP05_0011E': 'Population_35_to_44',
    'DP05_0037E': 'White_Population',
    'DP05_0047E': 'Asian_Population'
})

# Extract just the ZIP code from the ZCTA name
demo_data['ZCTA'] = demo_data['ZCTA'].str.extract(r'ZCTA5\s(\d+)')

# Convert numeric columns to numeric type
for col in ['Total_Population', 'Population_25_to_34', 'Population_35_to_44', 'White_Population', 'Asian_Population']:
    demo_data[col] = pd.to_numeric(demo_data[col], errors='coerce')

# Calculate the total young adult population (25-44)
demo_data['Population_25_to_44'] = demo_data['Population_25_to_34'] + demo_data['Population_35_to_44']

# Calculate percentages
demo_data['Percent_25_to_44'] = (demo_data['Population_25_to_44'] / demo_data['Total_Population'] * 100).round(1)
demo_data['Percent_White'] = (demo_data['White_Population'] / demo_data['Total_Population'] * 100).round(1)
demo_data['Percent_Asian'] = (demo_data['Asian_Population'] / demo_data['Total_Population'] * 100).round(1)

# Merge with the existing merged geodataframe to add this data
merged_gdf = merged_gdf.merge(demo_data[['ZCTA', 'Total_Population', 'Population_25_to_44', 
                                      'Percent_25_to_44', 'White_Population', 'Asian_Population',
                                      'Percent_White', 'Percent_Asian']], 
                              on='ZCTA', how='left')

# Read the housing characteristics data
housing_data = pd.read_csv(nyc_property_value_path)

# Select columns for median housing value and median rent
housing_data = housing_data[['NAME', 'DP04_0089E', 'DP04_0134E']]

# Rename columns for better readability
housing_data = housing_data.rename(columns={
    'NAME': 'ZCTA',
    'DP04_0089E': 'Median_Home_Value',
    'DP04_0134E': 'Median_Gross_Rent'
})

# Skip the first row which contains column descriptions
housing_data = housing_data.iloc[1:]

# Clean housing_data - extract just the ZIP code from the ZCTA name and convert to numeric
housing_data['ZCTA'] = housing_data['ZCTA'].str.extract(r'ZCTA5\s(\d+)')
housing_data['Median_Home_Value'] = pd.to_numeric(housing_data['Median_Home_Value'], errors='coerce')
housing_data['Median_Gross_Rent'] = pd.to_numeric(housing_data['Median_Gross_Rent'], errors='coerce')

merged_gdf = merged_gdf.merge(housing_data, on='ZCTA', how='left')
#save the merged geodataframe to a new file
output_path = 'cleaned_data/merged_nyc_raw_features.geojson'
# Create the directory if it doesn't exist
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Save the merged geodataframe to a GeoJSON file
merged_gdf.to_file(output_path, driver='GeoJSON')

print(f"Data successfully saved to {output_path}")