import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon, GeometryCollection
from shapely.ops import unary_union

# Read the shapefile
coast_path = "coastlines_2-2-0.shp/coastlines_2-2-0_shorelines_annual.shp"
gdf = gpd.read_file(coast_path)

# Filter to year = 2023
gdf = gdf[gdf["year"] == 2023]

# Confirm projected CRS
print("CRS:", gdf.crs)

print("Total coastline geometries:", len(gdf))

from shapely.ops import unary_union
from shapely.geometry import LineString, MultiLineString

# STEP 1: Combine all coastlines and create initial buffer (in chunks)
print("Combining all coastlines...")

buffer_distance = 5000  # 5km buffer to close gaps
coastlines = list(gdf.geometry)

buffered_pieces = []
print(f"Buffering each geometry individually with {buffer_distance}m buffer...")

for i, geom in enumerate(coastlines):
    try:
        if geom and not geom.is_empty:
            simplified = geom.simplify(50, preserve_topology=True)  # reduce detail
            buffered = simplified.buffer(buffer_distance, resolution=2)
            if not buffered.is_empty:
                buffered_pieces.append(buffered)
    except Exception as e:
        print(f"⚠ Skipping geometry {i} due to error: {e}")

print(f"Buffered {len(buffered_pieces)} geometries successfully.")

# Merge the buffered pieces
print("Merging buffered pieces...")
try:
    temp_buffer = unary_union(buffered_pieces)
except Exception as e:
    print(f"❌ Failed to union all buffered pieces: {e}")
    raise SystemExit("Aborting due to union failure")


# STEP 2: Extract the two largest polygons (mainland Australia and Tasmania)
if isinstance(temp_buffer, Polygon):
    # If we only got one polygon, just use it
    main_polygons = [temp_buffer]
    print("Only one main polygon created - using it as mainland")
elif isinstance(temp_buffer, MultiPolygon):
    # Sort polygons by area in descending order
    sorted_polygons = sorted(temp_buffer.geoms, key=lambda p: p.area, reverse=True)
    
    # Take the two largest (mainland Australia and Tasmania)
    if len(sorted_polygons) >= 2:
        main_polygons = sorted_polygons[:2]
        other_polygons = sorted_polygons[2:]
        print(f"Selected 2 largest polygons from {len(sorted_polygons)} polygons")
        print(f"Mainland area: {main_polygons[0].area:.2f}, Tasmania area: {main_polygons[1].area:.2f}")
    else:
        main_polygons = sorted_polygons
        other_polygons = []
        print(f"Only {len(sorted_polygons)} polygons created - using all as main")
else:
    raise TypeError(f"Unexpected geometry type: {type(temp_buffer)}")

# STEP 3: Process mainland Australia and Tasmania with asymmetric buffers
all_buffers = []

for i, polygon in enumerate(main_polygons):
    print(f"\nProcessing polygon {i+1}...")

    # Optional: Simplify to reduce complexity
    polygon = polygon.simplify(100, preserve_topology=True)

    # Landward buffer (inner/negative)
    landward_buffer = None
    for d in [-2000, -1000, -500]:  # try decreasing inward distances
        try:
            landward_buffer = polygon.buffer(d, resolution=2)
            if not landward_buffer.is_empty:
                print(f"✔ Landward buffer succeeded at {abs(d)}m for polygon {i+1}")
                break
        except Exception as e:
            print(f"⚠ Buffer {d}m failed: {e}")
    
    # Oceanward buffer (positive)
    try:
        oceanward_buffer = polygon.buffer(1000, resolution=2)
    except Exception as e:
        print(f"⚠ Oceanward buffer failed: {e}")
        oceanward_buffer = None

    # Add buffers
    if landward_buffer and not landward_buffer.is_empty:
        all_buffers.append(landward_buffer)
    if oceanward_buffer and not oceanward_buffer.is_empty:
        all_buffers.append(oceanward_buffer)

# STEP 4: Process all other islands with symmetric 500m buffer
if 'other_polygons' in locals() and other_polygons:
    print(f"Processing {len(other_polygons)} smaller islands with 500m buffer")
    for i, polygon in enumerate(other_polygons):
        island_buffer = polygon.buffer(500)  # 500m all around
        if not island_buffer.is_empty:
            all_buffers.append(island_buffer)
    
    print(f"Added buffers for {len(other_polygons)} smaller islands")

print("Total buffer geometries:", len(all_buffers))

# STEP 5: Union all buffers
try:
    combined_raw = unary_union(all_buffers)
    print("Type of combined result:", type(combined_raw))
    print("Is combined result empty?:", combined_raw.is_empty)
except Exception as e:
    print(f"Error in union operation: {e}")
    # Try one-by-one approach
    combined_raw = all_buffers[0]
    for buff in all_buffers[1:]:
        try:
            combined_raw = combined_raw.union(buff)
        except Exception as e:
            print(f"Skipping a buffer due to error: {e}")

# STEP 6: Flatten into individual polygons
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

# Make sure all geometries are valid
valid_geoms = []
for geom in flattened:
    if geom.is_valid:
        valid_geoms.append(geom)
    else:
        try:
            fixed = geom.buffer(0)  # Common trick to fix invalid geometries
            if fixed.is_valid and not fixed.is_empty:
                valid_geoms.append(fixed)
        except Exception:
            print("Skipping an invalid geometry")

print(f"Final valid geometries: {len(valid_geoms)}")

# STEP 7: Create output GeoDataFrame
gdf_out = gpd.GeoDataFrame(geometry=valid_geoms, crs=gdf.crs)
print("Geometry types in final output:", gdf_out.geometry.geom_type.unique())

# STEP 8: Save to shapefile
gdf_out.to_file("coast_buffer_please.shp")
print("Successfully saved buffer shapefile")