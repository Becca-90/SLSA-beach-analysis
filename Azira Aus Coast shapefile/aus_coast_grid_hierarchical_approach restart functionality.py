import arcpy
import sys
import os
import math

# Set up workspace and parameters
arcpy.env.workspace = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Aus Coast Map.gdb"
arcpy.env.overwriteOutput = True

# Input parameters - UPDATED FOR 5KM GRID TILES
coastline_buffer = "coast_water_combined_buffer"  # Your coastline buffer layer
output_workspace = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Output"  # Where to save results
cell_size = "10"  # Final fishnet cell size in meters
tile_size = 5000  # 5km tiles in meters

# Create output folder if it doesn't exist
if not os.path.exists(output_workspace):
    os.makedirs(output_workspace)

def create_processing_tiles(coastline_layer, tile_size_m):
    """Create a grid of tiles to process coastline in manageable chunks"""
    try:
        print(f"Creating {tile_size_m/1000}km × {tile_size_m/1000}km processing tiles...")
        
        # Get extent of coastline
        desc = arcpy.Describe(coastline_layer)
        extent = desc.extent
        
        print(f"  - Coastline extent: {extent.XMin:.0f}, {extent.YMin:.0f} to {extent.XMax:.0f}, {extent.YMax:.0f}")
        
        # Calculate number of tiles needed
        width = extent.XMax - extent.XMin
        height = extent.YMax - extent.YMin
        cols = int(math.ceil(width / tile_size_m))
        rows = int(math.ceil(height / tile_size_m))
        
        print(f"  - Creating {cols} × {rows} = {cols * rows} tiles")
        
        # Create fishnet for processing tiles
        tiles_fishnet = "processing_tiles_temp"
        origin_coord = f"{extent.XMin} {extent.YMin}"
        y_axis_coord = f"{extent.XMin} {extent.YMax}"
        corner_coord = f"{extent.XMax} {extent.YMax}"
        
        arcpy.management.CreateFishnet(
            out_feature_class=tiles_fishnet,
            origin_coord=origin_coord,
            y_axis_coord=y_axis_coord,
            cell_width=tile_size_m,
            cell_height=tile_size_m,
            number_rows="",
            number_columns="",
            corner_coord=corner_coord,
            labels="NO_LABELS",
            template="",
            geometry_type="POLYGON"
        )
        
        # Clip tiles to coastline area (only keep tiles that intersect coastline)
        tiles_clipped = "processing_tiles_clipped"
        arcpy.analysis.Clip(tiles_fishnet, coastline_layer, tiles_clipped)
        
        # Add tile ID field
        arcpy.management.AddField(tiles_clipped, "TILE_ID", "LONG")
        
        # Populate tile ID
        with arcpy.da.UpdateCursor(tiles_clipped, ["TILE_ID", "OID@"]) as cursor:
            for row in cursor:
                row[0] = row[1]  # Use ObjectID as tile ID
                cursor.updateRow(row)
        
        # Clean up temporary layer
        arcpy.management.Delete(tiles_fishnet)
        
        # Get actual count of tiles
        result = arcpy.management.GetCount(tiles_clipped)
        actual_tiles = int(result[0])
        
        print(f"  - Created {actual_tiles:,} tiles that intersect coastline")
        return tiles_clipped, actual_tiles
        
    except Exception as e:
        print(f"Error creating tiles: {str(e)}")
        return None, 0

def process_tile(tile_id, tiles_layer, coastline_layer):
    """Process a single tile"""
    try:
        print(f"Processing tile {tile_id}...")
        
        # Step 1: Select the current tile
        tile_layer = "tile_selected"
        arcpy.management.MakeFeatureLayer(tiles_layer, tile_layer, 
                                        f"TILE_ID = {tile_id}")
        
        # Check if tile exists
        result = arcpy.management.GetCount(tile_layer)
        count = int(result[0])
        
        if count == 0:
            print(f"  - Tile {tile_id} not found, skipping...")
            return False
        
        # Step 2: Clip coastline to this tile
        coastline_clipped = f"Coastline_Tile_{tile_id}"
        arcpy.analysis.Clip(coastline_layer, tile_layer, coastline_clipped)
        
        # Check if clipped coastline has features
        result = arcpy.management.GetCount(coastline_clipped)
        count = int(result[0])
        
        if count == 0:
            print(f"  - No coastline found in tile {tile_id}, skipping...")
            arcpy.management.Delete(coastline_clipped)
            return False
        
        # Step 3: Get extent of clipped coastline
        desc = arcpy.Describe(coastline_clipped)
        extent = desc.extent
        
        # Step 4: Create fishnet for this tile
        fishnet_output = f"Fishnet_Tile_{tile_id}"
        origin_coord = f"{extent.XMin} {extent.YMin}"
        y_axis_coord = f"{extent.XMin} {extent.YMax}"
        
        # Calculate number of rows and columns based on extent and cell size
        cell_size_num = float(cell_size)
        num_cols = int((extent.XMax - extent.XMin) / cell_size_num) + 1
        num_rows = int((extent.YMax - extent.YMin) / cell_size_num) + 1
        
        # Safety check for too many cells
        total_cells = num_cols * num_rows
        if total_cells > 1000000:  # 1 million cell safety limit
            print(f"  - Warning: Tile {tile_id} would create {total_cells:,} cells, skipping...")
            arcpy.management.Delete(coastline_clipped)
            return False
        
        print(f"  - Creating fishnet with {total_cells:,} cells...")
        
        # Create the fishnet with 10m cells
        arcpy.management.CreateFishnet(
            out_feature_class=fishnet_output,
            origin_coord=origin_coord,
            y_axis_coord=y_axis_coord,
            cell_width=cell_size,
            cell_height=cell_size,
            number_rows="",
            number_columns="",
            corner_coord=f"{extent.XMax} {extent.YMax}",
            labels="NO_LABELS",
            template=coastline_clipped,
            geometry_type="POLYGON"
        )
        
        # Step 5: Clip fishnet to actual coastline shape
        fishnet_clipped = f"Fishnet_Clipped_Tile_{tile_id}"
        arcpy.analysis.Clip(fishnet_output, coastline_clipped, fishnet_clipped)
        
        # Add tile ID to the fishnet for tracking
        arcpy.management.AddField(fishnet_clipped, "TILE_ID", "LONG")
        arcpy.management.CalculateField(fishnet_clipped, "TILE_ID", tile_id)
        
        # Clean up intermediate files
        arcpy.management.Delete(coastline_clipped)
        arcpy.management.Delete(fishnet_output)
        
        print(f"  - Successfully processed tile {tile_id}")
        return True
        
    except Exception as e:
        print(f"  - Error processing tile {tile_id}: {str(e)}")
        return False

def main():
    """Main processing function"""
    print("Starting automated coastal tile fishnet processing...")
    print(f"Tile size: {tile_size/1000}km × {tile_size/1000}km")
    print(f"Cell size: {cell_size}m")
    print(f"Output workspace: {output_workspace}")
    print("-" * 50)
    
    # Step 1: Create processing tiles
    tiles_layer, num_tiles = create_processing_tiles(coastline_buffer, tile_size)
    
    if tiles_layer is None or num_tiles == 0:
        print("Failed to create processing tiles. Exiting.")
        return
    
    # Step 2: Process each tile
    successful = 0
    failed = 0
    skipped = 0
    
    # RESUME FUNCTIONALITY - Set start_tile_id to resume from specific tile
    start_tile_id = 18862  # Change this to resume from a different tile (set to 1 for full run)
    
    # Get list of all tile IDs
    tile_ids = []
    with arcpy.da.SearchCursor(tiles_layer, ["TILE_ID"]) as cursor:
        for row in cursor:
            if row[0] >= start_tile_id:  # Only process tiles from start_tile_id onwards
                tile_ids.append(row[0])
    
    print(f"Found {len(tile_ids)} tiles to process (starting from tile {start_tile_id})")
    
    # Check what's already been processed
    existing_fishnets = arcpy.ListFeatureClasses("Fishnet_Clipped_Tile_*")
    processed_tiles = set()
    for fishnet in existing_fishnets:
        tile_num = fishnet.split("_")[-1]
        try:
            processed_tiles.add(int(tile_num))
        except:
            pass
    
    print(f"Found {len(processed_tiles)} already processed tiles")
    
    # Filter out already processed tiles
    remaining_tiles = [tid for tid in tile_ids if tid not in processed_tiles]
    print(f"Remaining tiles to process: {len(remaining_tiles)}")
    
    for i, tile_id in enumerate(remaining_tiles, 1):
        print(f"\n[{i}/{len(remaining_tiles)}] Processing tile {tile_id}")
        
        result = process_tile(tile_id, tiles_layer, coastline_buffer)
        if result:
            successful += 1
        elif result is False:
            failed += 1
        else:
            skipped += 1
    
    print("\n" + "="*50)
    print("PROCESSING COMPLETE")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Skipped (no coastline): {skipped}")
    print(f"Total tiles: {num_tiles}")
    
    # Step 3: Merge all results
    if successful > 0:
        print("\nMerging all fishnet results...")
        try:
            # Find all fishnet outputs
            arcpy.env.workspace = arcpy.env.workspace  # Reset to original workspace
            fishnet_layers = arcpy.ListFeatureClasses("Fishnet_Clipped_Tile_*")
            
            if len(fishnet_layers) > 0:
                merged_output = "Australia_Coastal_Fishnet_10m_5km_Tiles"
                arcpy.management.Merge(fishnet_layers, merged_output)
                print(f"Merged {len(fishnet_layers)} fishnet layers into {merged_output}")
                
                # Optionally delete individual tile fishnets to save space
                delete_individuals = input("\nDelete individual tile fishnets to save space? (y/n): ")
                if delete_individuals.lower() == 'y':
                    for layer in fishnet_layers:
                        arcpy.management.Delete(layer)
                    print("Individual tile fishnets deleted")
            
        except Exception as e:
            print(f"Error merging results: {str(e)}")
    
    # Clean up temporary tile layer
    if tiles_layer:
        try:
            arcpy.management.Delete(tiles_layer)
            print("Temporary tile layer cleaned up")
        except:
            pass
    
    print("\nScript completed!")

if __name__ == "__main__":
    main()