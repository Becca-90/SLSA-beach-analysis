import arcpy
import sys
import os

# Set up workspace and parameters
main_workspace = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Aus Coast Map.gdb"
arcpy.env.workspace = main_workspace
arcpy.env.overwriteOutput = True

# Input parameters
coastline_buffer = "Aus_coastline"  # Your coastline buffer layer
output_workspace = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Output"  # Where to save results
cell_size = "10"  # Grid cell size in meters
chunk_distance = "5000"  # Distance between cuts in meters (5km)

# Create output folder if it doesn't exist
if not os.path.exists(output_workspace):
    os.makedirs(output_workspace)

def create_coastline_grid_chunks():
    """Create a grid of chunks directly using fishnet - simpler approach"""
    try:
        print("Creating coastline grid chunks...")
        
        # Get the extent of the coastline
        desc = arcpy.Describe(coastline_buffer)
        extent = desc.extent
        
        # Create a fishnet with chunk-sized cells
        chunk_size = float(chunk_distance)  # 5000m = 5km
        
        print(f"Creating {chunk_size/1000}km grid chunks...")
        
        # Create fishnet for chunks
        chunk_fishnet = "Chunk_Fishnet"
        origin_coord = f"{extent.XMin} {extent.YMin}"
        y_axis_coord = f"{extent.XMin} {extent.YMax}"
        
        arcpy.management.CreateFishnet(
            out_feature_class=chunk_fishnet,
            origin_coord=origin_coord,
            y_axis_coord=y_axis_coord,
            cell_width=chunk_size,
            cell_height=chunk_size,
            number_rows="",
            number_columns="",
            corner_coord=f"{extent.XMax} {extent.YMax}",
            labels="NO_LABELS",
            template=coastline_buffer,
            geometry_type="POLYGON"
        )
        
        # Clip the fishnet to the coastline to get only coastal chunks
        coastline_chunks = "Coastline_Chunks"
        arcpy.analysis.Clip(chunk_fishnet, coastline_buffer, coastline_chunks)
        
        # Clean up
        arcpy.management.Delete(chunk_fishnet)
        
        # Get count of chunks
        result = arcpy.management.GetCount(coastline_chunks)
        count = int(result[0])
        
        print(f"Successfully created {count} coastline chunks")
        return coastline_chunks
        
    except Exception as e:
        print(f"Error creating coastline chunks: {str(e)}")
        return None

def cut_coastline_into_chunks():
    """Cut the coastline polygon into regular chunks - Updated to use simple grid approach"""
    try:
        print("Creating coastline chunks using grid approach...")
        
        # Use the simpler grid approach
        coastline_chunks = create_coastline_grid_chunks()
        
        if coastline_chunks is None:
            print("Failed to create chunks - returning original coastline")
            final_cut_coastline = "Coastline_Chunks"
            arcpy.management.CopyFeatures(coastline_buffer, final_cut_coastline)
            return final_cut_coastline
        
        return coastline_chunks
        
    except Exception as e:
        print(f"Error cutting coastline: {str(e)}")
        return None

def process_coastline_chunk(chunk_oid):
    """Process a single coastline chunk"""
    try:
        print(f"Processing chunk {chunk_oid}...")
        
        # Step 1: Select the current chunk
        chunk_layer = "chunk_selected"
        arcpy.management.MakeFeatureLayer("Coastline_Chunks", chunk_layer, 
                                        f"OBJECTID = {chunk_oid}")
        
        # Step 2: Get extent of chunk
        desc = arcpy.Describe(chunk_layer)
        extent = desc.extent
        
        # Step 3: Create fishnet
        fishnet_output = f"Fishnet_Chunk_{chunk_oid}"
        origin_coord = f"{extent.XMin} {extent.YMin}"
        y_axis_coord = f"{extent.XMin} {extent.YMax}"
        
        # Calculate number of rows and columns based on extent and cell size
        cell_size_num = float(cell_size)
        num_cols = int((extent.XMax - extent.XMin) / cell_size_num) + 1
        num_rows = int((extent.YMax - extent.YMin) / cell_size_num) + 1
        
        # Safety check for too many cells
        total_cells = num_cols * num_rows
        if total_cells > 10000000:  # 10 million cell safety limit
            print(f"  - Warning: Chunk {chunk_oid} would create {total_cells:,} cells, skipping...")
            return None
        
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
            template=coastline_buffer,  # Use the actual feature class instead of the layer
            geometry_type="POLYGON"
        )
        
        # Step 4: Clip fishnet to actual coastline chunk shape
        fishnet_clipped = f"Fishnet_Clipped_Chunk_{chunk_oid}"
        arcpy.analysis.Clip(fishnet_output, chunk_layer, fishnet_clipped)
        
        # Add chunk ID to the fishnet
        arcpy.management.AddField(fishnet_clipped, "CHUNK_ID", "LONG")
        with arcpy.da.UpdateCursor(fishnet_clipped, ["CHUNK_ID"]) as cursor:
            for row in cursor:
                row[0] = chunk_oid
                cursor.updateRow(row)
        
        # Clean up intermediate files
        arcpy.management.Delete(fishnet_output)
        
        print(f"  - Successfully processed chunk {chunk_oid}")
        return fishnet_clipped
        
    except Exception as e:
        print(f"  - Error processing chunk {chunk_oid}: {str(e)}")
        return None

def main():
    """Main processing function"""
    print("Starting automated coastal fishnet processing with regular chunking...")
    print(f"Cell size: {cell_size}m")
    print(f"Chunk interval: {float(chunk_distance)/1000}km")
    print(f"Main workspace: {main_workspace}")
    print(f"Output workspace: {output_workspace}")
    print("-" * 50)
    
    # Step 1: Cut coastline into regular chunks
    coastline_chunks = cut_coastline_into_chunks()
    if coastline_chunks is None:
        print("Failed to create coastline chunks. Exiting.")
        return
    
    # Step 2: Get list of all coastline chunks
    chunks = []
    with arcpy.da.SearchCursor(coastline_chunks, ["OBJECTID"]) as cursor:
        for row in cursor:
            chunks.append(row[0])
    
    print(f"Found {len(chunks)} coastline chunks to process")
    
    # Step 3: Process each chunk
    successful = 0
    failed = 0
    successful_layers = []
    
    for i, chunk_oid in enumerate(chunks, 1):
        print(f"\n[{i}/{len(chunks)}] Processing chunk {chunk_oid}")
        
        result = process_coastline_chunk(chunk_oid)
        if result:
            successful += 1
            successful_layers.append(result)
        else:
            failed += 1
    
    print("\n" + "="*50)
    print("PROCESSING COMPLETE")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Total chunks: {len(chunks)}")
    
    # Step 4: Merge all successful results
    if successful > 0 and len(successful_layers) > 0:
        print(f"\nMerging {len(successful_layers)} successful fishnet results...")
        try:
            # Create merged output in the main geodatabase
            merged_output = f"Australia_Coastal_Fishnet_10m_{int(float(chunk_distance)/1000)}km_Chunks"
            merged_path = os.path.join(main_workspace, merged_output)
            
            print(f"Merging to: {merged_path}")
            arcpy.management.Merge(successful_layers, merged_path)
            
            print(f"Successfully merged {len(successful_layers)} fishnet layers into {merged_output}")
            print(f"Merged layer saved to: {merged_path}")
            
            # Get count of final merged features
            result = arcpy.management.GetCount(merged_path)
            total_features = int(result[0])
            print(f"Total features in merged dataset: {total_features:,}")
            
            # Optional: Export to shapefile in output folder
            try:
                shapefile_path = os.path.join(output_workspace, f"{merged_output}.shp")
                arcpy.conversion.FeatureClassToShapefile(merged_path, output_workspace)
                print(f"Also exported to shapefile: {shapefile_path}")
            except Exception as e:
                print(f"Warning: Could not export to shapefile: {str(e)}")
            
            # Clean up individual chunk fishnets if desired
            print("\nCleaning up individual chunk fishnets...")
            for layer in successful_layers:
                try:
                    arcpy.management.Delete(layer)
                except:
                    pass
            
        except Exception as e:
            print(f"Error merging results: {str(e)}")
            print("Individual fishnet layers are still available in the geodatabase")
    else:
        print("No successful results to merge")
    
    # Clean up the coastline chunks if desired
    try:
        arcpy.management.Delete(coastline_chunks)
        print("Cleaned up temporary coastline chunks")
    except:
        pass
    
    print("\nScript completed!")
    print(f"Check your geodatabase: {main_workspace}")
    print(f"And output folder: {output_workspace}")

if __name__ == "__main__":
    main()