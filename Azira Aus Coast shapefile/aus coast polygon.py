import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, GeometryCollection
from shapely.ops import unary_union

# Read the shapefile ===
coast_path = "coastlines_2-2-0.shp/coastlines_2-2-0_shorelines_annual.shp"
gdf = gpd.read_file(coast_path)

# Filter to year = 2023 ===
# (Assuming the field is named "year" and is stored as string or integer)
gdf = gdf[gdf["year"] == 2023]

# === 3. Confirm projected CRS (should be EPSG:3577)
print("CRS:", gdf.crs)  # Should say GDA94 / Australia Albers (EPSG:3577)

# Separate main coastline from islands by length (tweak this threshold if needed)
gdf["length_m"] = gdf.geometry.length
main_coast = gdf[gdf["length_m"] > 100000]     # lines > 100 km = mainland coast
islands = gdf[gdf["length_m"] <= 100000]       # smaller lines = islands

# Flatten all lines into single LineStrings before buffering
def explode_lines(geom):
    if geom.geom_type == "LineString":
        return [geom]
    elif geom.geom_type == "MultiLineString":
        return list(geom.geoms)
    else:
        return []

print(main_coast.geometry.geom_type.value_counts())

# Apply this before buffering
main_lines = main_coast.geometry.explode(index_parts=False).reset_index(drop=True)

# Buffer main coast asymmetrically
landward = main_lines.apply(lambda geom: geom.parallel_offset(2000, side='left', join_style=2).buffer(1))
oceanward = main_lines.apply(lambda geom: geom.parallel_offset(500, side='right', join_style=2).buffer(1))

# Buffer islands symmetrically
island_buffers = islands.buffer(500)

# Combine everything
all_buffers = list(landward) + list(oceanward) + list(island_buffers)

# Filter to only valid polygons
polygon_geoms = [geom for geom in all_buffers if isinstance(geom, (Polygon, MultiPolygon))]

print("Total input geometries:", len(all_buffers))
print("Geometry types in all_buffers:")
print([geom.geom_type for geom in all_buffers[:10]])  # preview first 10

# STEP 1: Union everything (this might be a GeometryCollection)
combined_raw = unary_union(all_buffers)

print("Type of combined_raw:", type(combined_raw))
print("Is combined_raw empty?:", combined_raw.is_empty)

# STEP 2: Flatten everything into individual geometries
# This handles Polygon, MultiPolygon, GeometryCollection, etc.
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
print("Flattened geometry types:", [g.geom_type for g in flattened])

# STEP 3: Create GeoDataFrame with individual polygons
gdf_out = gpd.GeoDataFrame(geometry=flattened, crs=gdf.crs)
print("Geometry types in final output:", gdf_out.geometry.geom_type.unique())

# STEP 4: Save to shapefile
gdf_out.to_file("coast_buffer.shp")