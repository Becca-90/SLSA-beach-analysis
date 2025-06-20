import xarray as xr
from siphon.catalog import TDSCatalog
import pandas as pd
from datetime import datetime
import pytz
import time
from dask.distributed import Client

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

# Test location (latitude, longitude, datetime in AEST)
lat = float(-34.97326)  # Ensure it's a float
lon = float(138.5109)   # Ensure it's a float
aest_time = "2008-12-16 04:00:00"  # Make sure this matches the dataset time range
dt_utc = pd.to_datetime("2008-12-16 04:00:00")  # Use direct datetime that matches dataset

print(f"Using lat: {lat}, lon: {lon}, time: {dt_utc}")

print("Opening dataset...")
ds = xr.open_dataset("http://data-cbr.csiro.au/thredds/dodsC/catch_all/CMAR_CAWCR-Wave_archive/CAWCR_Wave_Hindcast_1979-2010/gridded/ww3.aus_4m.200812.nc")
print_dataset_info(ds)


sel = ds.sel( 
    latitude=-35.0,
    longitude=138.5,
    time=pd.to_datetime("2008-12-16 04:00:00"),
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
