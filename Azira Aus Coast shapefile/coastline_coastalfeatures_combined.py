import geopandas as gpd
from shapely.geometry import LineString, MultiLineString, Polygon, MultiPolygon, GeometryCollection
from shapely.ops import unary_union

# Read the shapefile
coast_path = "coastlines_2-2-0.shp/coastlines_2-2-0_shorelines_annual.shp"
gdf = gpd.read_file(coast_path)

# Filter to year = 2023
gdf = gdf[gdf["year"] == 2023]

# Confirm projected CRS
print("CRS:", gdf.crs)
print("Total coastline geometries:", len(gdf))

# STEP 1: Combine all coastlines and create initial buffer (in chunks)
print("Combining all coastlines...")

buffer_distance = 3000  # 5km buffer to close gaps
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

# STEP 3: Process mainland Australia and Tasmania with 2km buffers (both landward and seaward)
all_buffers = []

for i, polygon in enumerate(main_polygons):
    print(f"\nProcessing polygon {i+1}...")

    # Optional: Simplify to reduce complexity
    polygon = polygon.simplify(100, preserve_topology=True)

    # Landward buffer (inner/negative) - set to -2000m (2km)
    landward_buffer = None
    try:
        landward_buffer = polygon.buffer(-2000, resolution=2)
        print(f"✔ Landward buffer set at -2000m (2km) for polygon {i+1}")
    except Exception as e:
        print(f"⚠ Landward buffer failed: {e}")
        # Try a fallback buffer if the original fails
        try:
            landward_buffer = polygon.buffer(-1500, resolution=2)
            print(f"✔ Fallback landward buffer succeeded at -1500m for polygon {i+1}")
        except Exception as e:
            print(f"⚠ Fallback landward buffer also failed: {e}")
    
    # Oceanward buffer (positive) - set to 2000m (2km)
    try:
        oceanward_buffer = polygon.buffer(2000, resolution=2)
        print(f"✔ Seaward buffer set at 2000m (2km) for polygon {i+1}")
    except Exception as e:
        print(f"⚠ Seaward buffer failed: {e}")
        oceanward_buffer = None

    # Add buffers
    if landward_buffer and not landward_buffer.is_empty:
        all_buffers.append(landward_buffer)
    if oceanward_buffer and not oceanward_buffer.is_empty:
        all_buffers.append(oceanward_buffer)

# STEP 4: Process all other islands with symmetric 2km buffer
if 'other_polygons' in locals() and other_polygons:
    print(f"Processing {len(other_polygons)} smaller islands with 2km buffer")
    for i, polygon in enumerate(other_polygons):
        island_buffer = polygon.buffer(2000)  # 2km all around
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

print(f"Number of flattened coastline buffer geometries: {len(flattened)}")

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

print(f"Final valid coastline buffer geometries: {len(valid_geoms)}")

# STEP 7: Load and merge with water bodies
water_bodies_path = "coastal_geomorphological_features/1_Coastal Geomorphic Features.shp"
print(f"Loading water bodies from: {water_bodies_path}")

try:
    water_gdf = gpd.read_file(water_bodies_path)
    print(f"Loaded {len(water_gdf)} water body features")
    print(f"Water bodies CRS: {water_gdf.crs}")
    
    # Check if CRS needs to be aligned
    if water_gdf.crs != gdf.crs:
        print(f"Reprojecting water bodies from {water_gdf.crs} to {gdf.crs}")
        water_gdf = water_gdf.to_crs(gdf.crs)
    
    # Get water body geometries
    water_geoms = list(water_gdf.geometry)
    print(f"Processing {len(water_geoms)} water body geometries")
    
    # Union water bodies if needed
    try:
        water_union = unary_union(water_geoms)
        print("Successfully unioned water bodies")
    except Exception as e:
        print(f"Error unioning water bodies: {e}")
        # Fallback: Process individually
        water_union = water_geoms[0]
        for wg in water_geoms[1:]:
            try:
                water_union = water_union.union(wg)
            except:
                continue
    
    # Combine coastal buffer with water bodies
    print("Combining coastal buffer with water bodies...")
    coastal_buffer_union = unary_union(valid_geoms)
    
    try:
        final_union = coastal_buffer_union.union(water_union)
        print("Successfully merged coastal buffer with water bodies")
    except Exception as e:
        print(f"Error merging with water bodies: {e}")
        final_union = coastal_buffer_union
        print("Continuing with just the coastal buffer")
    
    # Flatten the final result
    final_geoms = []
    if isinstance(final_union, (Polygon, MultiPolygon)):
        final_geoms.append(final_union)
    elif isinstance(final_union, GeometryCollection):
        final_geoms.extend([g for g in final_union.geoms if isinstance(g, (Polygon, MultiPolygon))])
    
    print(f"Final combined geometries: {len(final_geoms)}")
    
except Exception as e:
    print(f"Error processing water bodies: {e}")
    print("Continuing with just the coastal buffer")
    final_geoms = valid_geoms

# STEP 8: Create output GeoDataFrame with the final geometries
gdf_out = gpd.GeoDataFrame(geometry=final_geoms, crs=gdf.crs)
print("Geometry types in final output:", gdf_out.geometry.geom_type.unique())

# STEP 9: Save to shapefile
output_file = "coast_water_combined_buffer.shp"
gdf_out.to_file(output_file)
print(f"Successfully saved combined buffer shapefile to {output_file}")

# Save as GeoJSON
geojson_output_file = "coast_water_combined_buffer.geojson"
gdf_out.to_file(geojson_output_file, driver="GeoJSON")
print(f"Successfully saved combined buffer as GeoJSON to {geojson_output_file}")