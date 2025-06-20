import rasterio
import numpy as np
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd

# Load image and stack bands
with rasterio.open("sentinel_bands.tif") as src:
    image = src.read()  # shape: (bands, rows, cols)
    profile = src.profile

# Reshape to (pixels, bands)
bands, rows, cols = image.shape
X = image.reshape(bands, -1).T  # shape: (n_pixels, n_bands)

# Load training points (must have lat, lon, and class column)
train_gdf = gpd.read_file("training_samples.geojson")
train_gdf = train_gdf.to_crs(profile["crs"])  # match raster CRS

# Extract pixel values under each point
coords = [(pt.x, pt.y) for pt in train_gdf.geometry]
row_col = [~src.transform * (x, y) for x, y in coords]
row_col = [(int(r), int(c)) for c, r in row_col]

train_pixels = [image[:, r, c] for r, c in row_col]
y = train_gdf["class"].values
X_train = np.array(train_pixels)

from sklearn.ensemble import RandomForestClassifier

clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y)

y_pred = clf.predict(X)
classified = y_pred.reshape(rows, cols)

with rasterio.open("classified_white_sand.tif", "w", **profile) as dst:
    dst.write(classified.astype(rasterio.uint8), 1)
