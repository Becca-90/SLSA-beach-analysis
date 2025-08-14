import arcpy
import os
import math

# Set up workspace and parameters
arcpy.env.workspace = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Aus Coast Map.gdb"
arcpy.env.overwriteOutput = True

# Input parameters - UPDATED FOR 5KM GRID TILES WITH BATCHING
coastline_buffer = "Aus_coastline"  # Your coastline buffer layer
output_workspace = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Fishnet\Aus Coast Fishnet.gdb"  # Where to save results
cell_size = "10"  # Final fishnet cell size in meters
tile_size = 5000  # 5km tiles in meters
batch_size = 3000  # Process 3000 tiles at a time

# Create output folder if it doesn't exist
if not os.path.exists(output_workspace):
    os.makedirs(output_workspace)

def create_processing_tiles(coastline_buffer, tile_size_m):
    """Create a grid of tiles to process coastline in manageable chunks"""
    try:
        print(f"Creating {tile_size_m/1000}km × {tile_size_m/1000}km processing tiles...")
        
        # Get extent of coastline
        desc = arcpy.Describe(coastline_buffer)
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
        arcpy.analysis.Clip(tiles_fishnet, coastline_buffer, tiles_clipped)
        
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

def process_tile(tile_id, tiles_layer, coastline_buffer):
    """Process a single tile"""
    try:
        print(f"  Processing tile {tile_id}...")
        
        # Step 1: Select the current tile
        tile_layer = "tile_selected"
        arcpy.management.MakeFeatureLayer(tiles_layer, tile_layer, 
                                        f"TILE_ID = {tile_id}")
        
        # Check if tile exists
        result = arcpy.management.GetCount(tile_layer)
        count = int(result[0])
        
        if count == 0:
            print(f"    - Tile {tile_id} not found, skipping...")
            return False
        
        # Step 2: Clip coastline to this tile
        coastline_clipped = f"Coastline_Tile_{tile_id}"
        arcpy.analysis.Clip(coastline_buffer, tile_layer, coastline_clipped)
        
        # Check if clipped coastline has features
        result = arcpy.management.GetCount(coastline_clipped)
        count = int(result[0])
        
        if count == 0:
            print(f"    - No coastline found in tile {tile_id}, skipping...")
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
            print(f"    - Warning: Tile {tile_id} would create {total_cells:,} cells, skipping...")
            arcpy.management.Delete(coastline_clipped)
            return False
        
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
        
        return True
        
    except Exception as e:
        print(f"    - Error processing tile {tile_id}: {str(e)}")
        return False

def process_batch(batch_tiles, tiles_layer, coastline_buffer, batch_num, total_batches):
    """Process a batch of tiles and merge the results"""
    print(f"\n{'='*60}")
    print(f"PROCESSING BATCH {batch_num}/{total_batches}")
    print(f"Tiles: {batch_tiles[0]} to {batch_tiles[-1]} ({len(batch_tiles)} tiles)")
    print(f"{'='*60}")
    
    successful_tiles = []
    
    # Process each tile in the batch
    for i, tile_id in enumerate(batch_tiles, 1):
        print(f"[{i}/{len(batch_tiles)}] ", end="")
        result = process_tile(tile_id, tiles_layer, coastline_buffer)
        if result:
            successful_tiles.append(f"Fishnet_Clipped_Tile_{tile_id}")
    
    print(f"\nBatch {batch_num} processing complete: {len(successful_tiles)} successful tiles")
    
    # Merge successful tiles in this batch
    if successful_tiles:
        batch_output = f"BATCH_{batch_num:02d}_MERGED"
        print(f"Merging {len(successful_tiles)} tiles into {batch_output}...")
        
        try:
            arcpy.management.Merge(successful_tiles, batch_output)
            
            # Get count of merged features
            result = arcpy.management.GetCount(batch_output)
            feature_count = int(result[0])
            print(f"✓ Batch {batch_num} merged: {feature_count:,} features")
            
            # Clean up individual tile fishnets to save space
            print(f"Cleaning up {len(successful_tiles)} individual tile fishnets...")
            cleaned_count = 0
            for tile_fishnet in successful_tiles:
                try:
                    arcpy.management.Delete(tile_fishnet)
                    cleaned_count += 1
                except:
                    print(f"    - Could not delete {tile_fishnet}")
            
            print(f"✓ Cleaned up {cleaned_count}/{len(successful_tiles)} individual tiles")
            
            return batch_output, len(successful_tiles)
            
        except Exception as e:
            print(f"✗ Error merging batch {batch_num}: {str(e)}")
            return None, len(successful_tiles)
    else:
        print(f"No successful tiles in batch {batch_num} to merge")
        return None, 0

def main():
    """Main processing function with batching"""
    print("Starting BATCH coastal tile fishnet processing...")
    print(f"Batch size: {batch_size} tiles per batch")
    print(f"Tile size: {tile_size/1000}km × {tile_size/1000}km")
    print(f"Cell size: {cell_size}m")
    print(f"Output workspace: {output_workspace}")
    print("-" * 50)
    
    # Step 1: Create processing tiles
    tiles_layer, num_tiles = create_processing_tiles(coastline_buffer, tile_size)
    
    if tiles_layer is None or num_tiles == 0:
        print("Failed to create processing tiles. Exiting.")
        return
    
    # Step 2: Get all tile IDs and organize into batches
    tile_ids = []
    with arcpy.da.SearchCursor(tiles_layer, ["TILE_ID"]) as cursor:
        for row in cursor:
            tile_ids.append(row[0])
    
    tile_ids.sort()  # Process in order
    
    # Create batches
    batches = []
    for i in range(0, len(tile_ids), batch_size):
        batch = tile_ids[i:i + batch_size]
        batches.append(batch)
    
    print(f"Created {len(batches)} batches from {len(tile_ids)} tiles")
    print(f"Batch sizes: {[len(batch) for batch in batches]}")
    
    # Step 3: Process each batch
    batch_outputs = []
    total_successful = 0
    
    for batch_num, batch_tiles in enumerate(batches, 1):
        batch_output, successful_count = process_batch(
            batch_tiles, tiles_layer, coastline_buffer, batch_num, len(batches)
        )
        
        if batch_output:
            batch_outputs.append(batch_output)
        
        total_successful += successful_count
        
        print(f"Batch {batch_num} summary: {successful_count} tiles processed")
    
    # Step 4: Final merge of all batches
    print(f"\n{'='*60}")
    print("FINAL MERGE")
    print(f"{'='*60}")
    
    if len(batch_outputs) > 1:
        print(f"Merging {len(batch_outputs)} batch outputs into final result...")
        try:
            final_output = "Australia_Coastal_Fishnet_10m_Complete"
            arcpy.management.Merge(batch_outputs, final_output)
            
            # Get final count
            result = arcpy.management.GetCount(final_output)
            final_count = int(result[0])
            print(f"✓ Final merge complete: {final_count:,} features")
            
            # Clean up batch outputs
            print(f"Cleaning up {len(batch_outputs)} batch files...")
            for batch_output in batch_outputs:
                try:
                    arcpy.management.Delete(batch_output)
                    print(f"  ✓ Deleted {batch_output}")
                except:
                    print(f"  ✗ Could not delete {batch_output}")
            
            print(f"\n🎉 SUCCESS!")
            print(f"Final output: {final_output}")
            print(f"Total features: {final_count:,}")
            
        except Exception as e:
            print(f"✗ Error in final merge: {str(e)}")
            print("Individual batch files are preserved for manual merging")
            
    elif len(batch_outputs) == 1:
        # Only one batch - rename it to final output
        try:
            final_output = "Australia_Coastal_Fishnet_10m_Complete"
            arcpy.management.Rename(batch_outputs[0], final_output)
            
            result = arcpy.management.GetCount(final_output)
            final_count = int(result[0])
            
            print(f"✓ Single batch renamed to final output: {final_count:,} features")
            print(f"Final output: {final_output}")
            
        except Exception as e:
            print(f"Error renaming final output: {str(e)}")
    else:
        print("No successful batches to merge")
    
    # Step 5: Summary
    print(f"\n{'='*60}")
    print("PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total tiles created: {num_tiles}")
    print(f"Tiles processed successfully: {total_successful}")
    print(f"Batches created: {len(batches)}")
    print(f"Batch outputs created: {len(batch_outputs)}")
    
    # Clean up temporary tile layer
    if tiles_layer:
        try:
            arcpy.management.Delete(tiles_layer)
            print("✓ Temporary tile layer cleaned up")
        except:
            pass
    
    print("\n🏁 Script completed!")

if __name__ == "__main__":
    main()