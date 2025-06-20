import cdsapi
import xarray as xr
import numpy as np
import pandas as pd
from pathlib import Path

# === CONFIG ===
years = list(range(2005, 2025))
point_file = "all_weather_data.csv"
batch_size = 100  # Adjust as needed
output_dir = Path("era5_daily_data_batched")
output_dir.mkdir(exist_ok=True)

variables = [
    "significant_height_of_combined_wind_waves_and_swell",
    "mean_wave_direction",
    "mean_wave_period",
    "mean_sea_level_pressure",
    "10m_u_component_of_wind",
    "10m_v_component_of_wind",
    "2m_temperature",
    "2m_dewpoint_temperature",
]

# === LOAD GPS POINTS ===
gps_df = pd.read_csv(point_file).reset_index(drop=True)
gps_points = list(zip(gps_df["latitude"], gps_df["longitude"]))

# === ERA5 Client ===
c = cdsapi.Client()
c.status()

for year in years:
    out_file = output_dir / f"era5_{year}.nc"
    if not out_file.exists():
        print(f"Downloading ERA5 data for {year}...")
        c.retrieve(
            "reanalysis-era5-single-levels",
            {
                "product_type": "reanalysis",
                "format": "netcdf",
                "variable": variables,
                "year": str(year),
                "month": [f"{m:02d}" for m in range(1, 13)],
                "day": [f"{d:02d}" for d in range(1, 32)],
                "time": [f"{h:02d}:00" for h in range(24)],
                "area": [-10, 110, -45, 155],
            },
            str(out_file)
        )

    print(f"Processing {year}...")
    ds = xr.open_dataset(out_file)
    daily_ds = ds.resample(time="1D").mean()

    # Loop over batches of points
    for start in range(0, len(gps_points), batch_size):
        batch = gps_points[start:start+batch_size]
        print(f"  Extracting points {start} to {start + len(batch) - 1}")

        batch_data = []

        for i, (lat, lon) in enumerate(batch, start=start):
            point_ds = daily_ds.sel(latitude=lat, longitude=lon, method="nearest")
            df = point_ds.to_dataframe().reset_index()

            df["wind_speed"] = np.sqrt(df["u10"]**2 + df["v10"]**2)
            df["wind_dir"] = (np.arctan2(df["u10"], df["v10"]) * 180 / np.pi) % 360
            df["rel_humidity"] = 100 * (
                np.exp((17.625 * df["d2m"]) / (243.04 + df["d2m"])) /
                np.exp((17.625 * df["t2m"]) / (243.04 + df["t2m"]))
            )

            df_out = df[[
                "time",
                "significant_height_of_combined_wind_waves_and_swell",
                "mean_wave_direction",
                "mean_wave_period",
                "mean_sea_level_pressure",
                "t2m", "d2m", "rel_humidity",
                "wind_speed", "wind_dir"
            ]].copy()

            df_out.columns = [
                "date", "wave_height", "wave_dir", "wave_period",
                "mslp", "temp", "dewpoint", "rel_humidity",
                "wind_speed", "wind_dir"
            ]
            df_out["lat"] = lat
            df_out["lon"] = lon
            df_out["point_id"] = i

            batch_data.append(df_out)

        # Combine batch and save to disk
        batch_df = pd.concat(batch_data, ignore_index=True)
        out_path = output_dir / f"era5_daily_y{year}_p{start}-{start+len(batch)-1}.parquet"
        batch_df.to_parquet(out_path, index=False)

        print(f"  ✅ Saved batch to {out_path}")
        del batch_df, batch_data  # Clear memory

print("🎉 All done! Batched output stored in:", output_dir)

