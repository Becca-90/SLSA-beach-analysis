import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, GeometryCollection, LineString
from shapely.ops import unary_union

# Read the shapefile ===
coast_path = "coastlines_2-2-0.shp/coastlines_2-2-0_shorelines_annual.shp"
gdf = gpd.read_file(coast_path)

# Filter to year = 2023 ===
gdf = gdf[gdf["year"] == 2023]

# === 3. Confirm projected CRS (should be EPSG:3577)
print("CRS:", gdf.crs)  # Should say GDA94 / Australia Albers (EPSG:3577)

# Separate main coastline from islands by length
gdf["length_m"] = gdf.geometry.length
main_coast = gdf[gdf["length_m"] > 260000]     # lines > 260 km = mainland coast
islands = gdf[gdf["length_m"] <= 260000]       # smaller lines = islands

print("Mainland coast geometries:", len(main_coast))
print("Island geometries:", len(islands))

# Step 1: Create a closed polygon from mainland coast
# Combine all mainland coast lines
combined_lines = unary_union(main_coast.geometry)
print("Combined mainland lines type:", type(combined_lines))

# Create a small buffer to connect any small gaps in the coastline
temp_buffer = combined_lines.buffer(5000)
print("Temporary buffer type:", type(temp_buffer))

# Extract the largest polygon if we get a MultiPolygon
if isinstance(temp_buffer, Polygon):
    mainland_polygon = temp_buffer
elif isinstance(temp_buffer, MultiPolygon):
    # Take the largest polygon if there are multiple
    mainland_polygon = max(temp_buffer.geoms, key=lambda p: p.area)
else:
    raise TypeError(f"Unexpected geometry type: {type(temp_buffer)}")

print("Successfully created mainland polygon")

# Step 2: Create buffers using the mainland polygon
# Landward buffer (inner/negative buffer)
landward_buffer = mainland_polygon.buffer(-2000)  # 2km inward
# Oceanward buffer (outer/positive buffer)
oceanward_buffer = mainland_polygon.buffer(500)   # 500m outward

print("Created inner and outer buffers for mainland")

# Step 3: Buffer islands symmetrically
island_buffers = islands.geometry.buffer(500)
print("Created buffers for", len(island_buffers), "islands")

# Step 4: Combine all buffers
all_buffers = []

# Add mainland buffers if they're not empty
if not landward_buffer.is_empty:
    all_buffers.append(landward_buffer)
if not oceanward_buffer.is_empty:
    all_buffers.append(oceanward_buffer)

# Add island buffers
for buffer in island_buffers:
    if not buffer.is_empty:
        all_buffers.append(buffer)

print("Total input geometries:", len(all_buffers))
print("Geometry types in all_buffers:")
print([geom.geom_type for geom in all_buffers[:5]])  # preview first 5

# Step 5: Union everything
combined_raw = unary_union(all_buffers)
print("Type of combined result:", type(combined_raw))
print("Is combined result empty?:", combined_raw.is_empty)

# Step 6: Flatten into individual polygons
flattened = []
if isinstance(combined_raw, (Polygon, MultiPolygon)):
    flattened.append(combined_raw)
elif isinstance(combined_raw, GeometryCollection):
    flattened.extend([g for g in combined_raw.geoms if isinstance(g, (Polygon, MultiPolygon))])
elif hasattr(combined_raw, '__iter__'):
    for g in combined_raw:
        if isinstance(g, (Polygon, MultiPolygon)):
            flattened.append(g)
        elif isinstance(g, GeometryCollection):
            flattened.extend([geom for geom in g.geoms if isinstance(geom, (Polygon, MultiPolygon))])

print(f"Number of flattened geometries: {len(flattened)}")
print("Flattened geometry types:", [g.geom_type for g in flattened[:5]])

# Step 7: Create output GeoDataFrame
gdf_out = gpd.GeoDataFrame(geometry=flattened, crs=gdf.crs)
print("Geometry types in final output:", gdf_out.geometry.geom_type.unique())

# Step 8: Save to shapefile
gdf_out.to_file("coast_buffer_try4.shp")
print("Successfully saved buffer shapefile")
