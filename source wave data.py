import xarray as xr
from siphon.catalog import TDSCatalog
import pandas as pd
from datetime import datetime
import pytz
import time
from dask.distributed import Client
import numpy as np
import matplotlib.pyplot as plt

#cat = TDSCatalog("http://data-cbr.csiro.au/thredds/catalog/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_aggregate/gridded/catalog.xml")
cat = TDSCatalog("http://data-cbr.csiro.au/thredds/catalog/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_1979-2010/gridded/catalog.xml")
print("\n".join(cat.datasets.keys()))

filelist=[x for x in cat.datasets if x.startswith('ww3.aus_4m.')]
#DAProot='http://data-cbr.csiro.au/thredds/dodsC/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_aggregate/gridded/'
DAProot='http://data-cbr.csiro.au/thredds/dodsC/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_1979-2010/gridded/'
path = [ DAProot+f for f in filelist]

path.sort()

data = xr.open_mfdataset(path, combine='by_coords',parallel=True)#,chunks={"time": 744}) #This bit is slooooow! Start it and wander off for a long while (20min-2hrs?).
data

#<xarray.Dataset>
#Dimensions:    (latitude: 796, longitude: 1126, time: 280512)
#Coordinates:
#  * longitude  (longitude) float32 100.0 100.066666 ... 174.9337 175.00038
#  * latitude   (latitude) float32 -50.0 -49.933334 ... 2.933598 3.000265
#  * time       (time) datetime64[ns] 1979-01-01 ... 2010-12-31T23:00:00.000013440

from dask.distributed import Client
client = Client()
print(client)

hs=data.hs.sel(latitude=-35.023,longitude=138.46,method='nearest').load(scheduler='processes')

hs


#################### TEST WITH SINGLE SITE ########################

def convert_to_utc(aest_str):
    aest = pytz.timezone('Australia/Sydney')
    dt_local = aest.localize(pd.to_datetime(aest_str))
    return dt_local.astimezone(pytz.utc)

# Print dataset time range and bounds to debug
def print_dataset_info(ds):
    print("Dataset time range:", ds.time.min().values, "to", ds.time.max().values)
    print("Latitude bounds:", ds.latitude.min().values, ds.latitude.max().values)
    print("Longitude bounds:", ds.longitude.min().values, ds.longitude.max().values)

def get_wave_data(ds, lat_str, lon_str, time_str):
    """
    Get wave data for a specific location and time from the dataset.
    Handles string inputs from dataframe and finds valid data points.
    """
    try:
        # Convert to appropriate types
        lat = float(lat_str)
        lon = float(lon_str)
        time = pd.to_datetime(time_str)
        
        # Make sure coordinates are within dataset bounds
        if (lat < ds.latitude.min().values or lat > ds.latitude.max().values or
            lon < ds.longitude.min().values or lon > ds.longitude.max().values):
            print(f"Warning: Coordinates ({lat}, {lon}) outside dataset bounds")
            return None
            
        # Find nearest available coordinates in the dataset
        lat_diff = abs(ds.latitude.values - lat)
        lon_diff = abs(ds.longitude.values - lon)
        nearest_lat_idx = np.argmin(lat_diff)
        nearest_lon_idx = np.argmin(lon_diff)
        
        nearest_lat = float(np.ravel(ds.latitude.values)[nearest_lat_idx])
        nearest_lon = float(np.ravel(ds.longitude.values)[nearest_lon_idx])
        
        # Select data using nearest coordinates
        sel_data = ds.sel(
            latitude=nearest_lat,
            longitude=nearest_lon,
            time=time,
            method='nearest'
        )
        
        # Check if the data point has valid values
        if np.isnan(sel_data['hs'].values).all():
            print(f"Warning: No valid data at ({nearest_lat}, {nearest_lon})")
            return None
            
        # Safely extract scalar values
        def extract_scalar(arr):
            return arr.item() if arr.size == 1 else arr.flatten()[0]

        hs_val = extract_scalar(sel_data['hs'].values)
        dir_val = extract_scalar(sel_data['dir'].values)
        tm0m1_val = extract_scalar(sel_data['tm0m1'].values)

        return {
            "hs": hs_val,
            "dir": dir_val,
            "tm0m1": tm0m1_val,
            "used_lat": nearest_lat,
            "used_lon": nearest_lon,
            "used_time": sel_data.time.values
        }
    
    except Exception as e:
        print(f"Error getting wave data: {e}")
        return None

# Test location (latitude, longitude, datetime in AEST)
lat = float(-34.97326)  # Ensure it's a float
lon = float(138.5109)   # Ensure it's a float
aest_time = "2008-12-16 04:00:00"  # Make sure this matches the dataset time range
dt_utc = pd.to_datetime("2008-12-16 04:00:00")  # Use direct datetime that matches dataset

print(f"Using lat: {lat}, lon: {lon}, time: {dt_utc}")

print("Opening dataset...")
ds = xr.open_dataset("https://data-cbr.csiro.au/thredds/dodsC/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_aggregate/gridded/ww3.aus_4m.202406.nc")
print_dataset_info(ds)

import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Load dataset
ds = xr.open_dataset("https://data-cbr.csiro.au/thredds/dodsC/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_aggregate/gridded/ww3.aus_4m.202410.nc")

# Pick a time index to visualize (or use method='nearest' on a datetime)
sample_time = ds.time[0]  # You can adjust this index or use .sel(time=...)

# Get wave height (hs) at that time
wave = ds.hs.sel(time=sample_time)

# Create plot
fig = plt.figure(figsize=(10, 6))
ax = plt.axes(projection=ccrs.PlateCarree())
wave.plot(ax=ax, transform=ccrs.PlateCarree(), cmap='viridis', cbar_kwargs={'label': 'Wave Height (m)'})

# Add features
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS, linestyle=':')
ax.set_title(f"Significant Wave Height\n{str(sample_time.values)}")

# Target coordinates (example)
target_lat = -33.573156
target_lon = 151.286056

# Overlay target location
ax.plot(target_lon, target_lat, 'ro', markersize=8, transform=ccrs.PlateCarree(), label='Target Location')
ax.legend()

plt.show()


# Load a few rows from your CSV file
print("Loading sample data from CSV...")
# Replace 'all_weather_data.csv' with your actual file path
df = pd.read_csv('all_weather_data.csv')

# Take just the first 5 rows for testing
sample_df = df.head(5)
print(f"Loaded {len(sample_df)} sample rows")
print(sample_df[['datetime', 'latitude', 'longitude']])

# Test the function with the sample rows
results = []
for idx, row in sample_df.iterrows():
    print(f"\nProcessing row {idx+1}:")
    print(f"Coordinates: {row['latitude']}, {row['longitude']}, Time: {row['datetime']}")
    
    result = get_wave_data(ds, row['latitude'], row['longitude'], row['datetime'])
    
    if result:
        results.append(result)
        print("Successfully retrieved wave data:")
        print(f"Wave height: {result['hs']:.2f} m")
        print(f"Wave direction: {result['dir']:.1f}°")
        print(f"Wave period: {result['tm0m1']:.1f} s")
        print(f"Using coordinates: {result['used_lat']}, {result['used_lon']}")
        print(f"Using time: {result['used_time']}")
    else:
        print("Failed to retrieve wave data for this location/time")

# Create a DataFrame with the results
if results:
    results_df = pd.DataFrame(results)
    print("\nResults summary:")
    print(results_df)

####################################################################################

sel = ds.sel( 
    latitude=-35.0,
    longitude=138.5,
    time=pd.to_datetime("2024-06-10 04:00:00"),
    method='nearest'
)

# Now try with the variables
sel_var = ds.sel(
    latitude=lat,
    longitude=lon,
    time=dt_utc,
    method='nearest'
)

# Compare results
print("\nHardcoded values result:")
print(f"Significant wave height (hs): {float(sel['hs'].values):.2f} m")
print(f"Mean wave direction (dir): {float(sel['dir'].values):.1f}°")
print(f"Mean wave period (t0m1): {float(sel['tm0m1'].values):.1f} s")

print("\nVariable values result:")
print(f"Significant wave height (hs): {float(sel_var['hs'].values):.2f} m")
print(f"Mean wave direction (dir): {float(sel_var['dir'].values):.1f}°")
print(f"Mean wave period (t0m1): {float(sel_var['tm0m1'].values):.1f} s")

print(ds.latitude.min().values, ds.latitude.max().values)
print(ds.longitude.min().values, ds.longitude.max().values)
print(ds["hs"].sel(latitude=lat, longitude=lon, method="nearest"))


#################### UDPATED FOR ALL LOCATIONS #####################

# Start Dask client
client = Client()
print(client)

# Load CAWCR dataset
print("Loading CAWCR Wave Hindcast dataset...")
dods_url = "http://data-cbr.csiro.au/thredds/dodsC/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_1979-2010/gridded/ww3.aus_4m"
ds = xr.open_dataset(dods_url)

# Load CSV
df = pd.read_csv("all_weather_data.csv")
required_cols = {"latitude", "longitude", "datetime"}
if not required_cols.issubset(df.columns):
    raise ValueError("CSV must contain 'latitude', 'longitude', 'datetime'")

results = []

for idx, row in df.iterrows():
    lat = float(row['latitude'])
    lon = float(row['longitude'])
    dt_utc = convert_to_utc(row['datetime'])

    try:
        sel = ds.sel(
            latitude=lat,
            longitude=lon,
            time=dt_utc,
            method="nearest"
        )

        results.append({
            "latitude": lat,
            "longitude": lon,
            "datetime": row['datetime'],
            "significant_wave_height": float(sel["hs"].values),
            "mean_wave_direction": float(sel["dir"].values),
            "mean_wave_period": float(sel["t0m1"].values),
            "source": "CSIRO CAWCR Hindcast"
        })
    except Exception as e:
        results.append({
            "latitude": lat,
            "longitude": lon,
            "datetime": row['datetime'],
            "error": str(e)
        })

    time.sleep(0.05)

# Save results
pd.DataFrame(results).to_csv("cawcr_wave_results.csv", index=False)
print("Saved to cawcr_wave_results.csv")
