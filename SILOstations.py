import urllib.request
import urllib.parse
import pandas as pd
import requests

# Base URL for the SILO station metadata API
base_url = "https://siloapi.longpaddock.qld.gov.au/stations"

# Define query parameters (if needed)
params = {
    "bbox": "-44,112,-10,154",  # Bounding box for Australia
}

# Make the GET request
response = requests.get(base_url, params=params)

# Check for successful response
if response.status_code == 200:
    station_data = response.json()  # Parse JSON response
    
    # Convert to a pandas DataFrame for easy handling
    stations_df = pd.DataFrame(station_data['stations'])  # Adjust key based on actual API response
    print(stations_df.head())  # Print the first few rows
else:
    print(f"Error: {response.status_code}, {response.text}")