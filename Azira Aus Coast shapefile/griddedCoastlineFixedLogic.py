import arcpy
import sys
import os

# Set up workspace and parameters
main_workspace = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Aus Coast Map.gdb"
arcpy.env.workspace = main_workspace
arcpy.env.overwriteOutput = True

# Input parameters - UPDATED FOR SUBURBS
coastline_buffer = "Aus_coastline"  # Your coastline buffer layer
lga_boundaries = "Suburbs"  # Your suburb boundaries layer
lga_name_field = "SAL_NAME21"  # Field containing suburb names
output_workspace = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Output"  # Where to save results
cell_size = "10"  # Grid cell size in meters

# Create output folder if it doesn't exist
if not os.path.exists(output_workspace):
    os.makedirs(output_workspace)

def clean_filename(name):
    """Clean suburb name to be valid filename"""
    # Remove invalid characters and replace spaces
    invalid_chars = '<>:"/\\|?*()[]{}-,.'
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name.replace(' ', '_').replace('__', '_').strip('_')

def process_lga(suburb_name):
    """Process a single suburb"""
    try:
        print(f"Processing {suburb_name}...")
        
        # Clean the suburb name for file naming
        clean_name = clean_filename(suburb_name)
        
        # Step 1: Select the current suburb
        suburb_layer = "suburb_selected"
        arcpy.management.MakeFeatureLayer(lga_boundaries, suburb_layer, 
                                        f"{lga_name_field} = '{suburb_name}'")
        
        # Step 2: Clip coastline to this suburb
        coastline_clipped = f"Coastline_{clean_name}"
        arcpy.analysis.Clip(coastline_buffer, suburb_layer, coastline_clipped)
        
        # Check if clipped coastline has features
        result = arcpy.management.GetCount(coastline_clipped)
        count = int(result[0])
        
        if count == 0:
            print(f"  - No coastline found in {suburb_name}, skipping...")
            arcpy.management.Delete(coastline_clipped)
            return "skipped"
        
        # Step 3: Get extent of clipped coastline
        desc = arcpy.Describe(coastline_clipped)
        extent = desc.extent
        
        # Step 4: Create fishnet
        fishnet_output = f"Fishnet_{clean_name}"
        origin_coord = f"{extent.XMin} {extent.YMin}"
        y_axis_coord = f"{extent.XMin} {extent.YMax}"
        
        # Calculate number of rows and columns based on extent and cell size
        cell_size_num = float(cell_size)
        num_cols = int((extent.XMax - extent.XMin) / cell_size_num) + 1
        num_rows = int((extent.YMax - extent.YMin) / cell_size_num) + 1
        
        # Safety check for too many cells - keep 10m for suburbs
        total_cells = num_cols * num_rows
        if total_cells > 10000000:  # 10 million cell safety limit for suburbs
            print(f"  - Warning: {suburb_name} would create {total_cells:,} cells, skipping...")
            arcpy.management.Delete(coastline_clipped)
            return "failed"
        
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
        fishnet_clipped = f"Fishnet_Clipped_{clean_name}"
        arcpy.analysis.Clip(fishnet_output, coastline_clipped, fishnet_clipped)
        
        # Clean up intermediate files
        arcpy.management.Delete(coastline_clipped)
        arcpy.management.Delete(fishnet_output)
        
        print(f"  - Successfully processed {suburb_name}")
        return "success"
        
    except Exception as e:
        print(f"  - Error processing {suburb_name}: {str(e)}")
        return "failed"

def main():
    """Main processing function"""
    print("Starting automated coastal suburb fishnet processing...")
    print(f"Cell size: {cell_size}m")
    print(f"Main workspace: {main_workspace}")
    print(f"Output workspace: {output_workspace}")
    print("-" * 50)
    
    # Get list of all suburbs
    suburb_names = []
    with arcpy.da.SearchCursor(lga_boundaries, [lga_name_field]) as cursor:
        for row in cursor:
            if row[0] is not None:  # Skip null values
                suburb_names.append(row[0])
    
    print(f"Found {len(suburb_names)} suburbs to process")
    
    # Process each suburb
    successful = 0
    failed = 0
    skipped = 0
    successful_layers = []
    
    for i, suburb_name in enumerate(suburb_names, 1):
        print(f"\n[{i}/{len(suburb_names)}] Processing: {suburb_name}")
        
        result = process_lga(suburb_name)
        if result == "success":
            successful += 1
            # Track successful layer names for merging
            clean_name = clean_filename(suburb_name)
            successful_layers.append(f"Fishnet_Clipped_{clean_name}")
        elif result == "failed":
            failed += 1
        elif result == "skipped":
            skipped += 1
    
    print("\n" + "="*50)
    print("PROCESSING COMPLETE")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Skipped (no coastline): {skipped}")
    print(f"Total: {len(suburb_names)}")
    
    # Merge all successful results
    if successful > 0 and len(successful_layers) > 0:
        print(f"\nMerging {len(successful_layers)} successful fishnet results...")
        try:
            # Create merged output in the main geodatabase
            merged_output = "Australia_Coastal_Fishnet_10m_Suburbs"
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
            
        except Exception as e:
            print(f"Error merging results: {str(e)}")
            print("Individual fishnet layers are still available in the geodatabase")
    else:
        print("No successful results to merge")
    
    print("\nScript completed!")
    print(f"Check your geodatabase: {main_workspace}")
    print(f"And output folder: {output_workspace}")

if __name__ == "__main__":
    main()