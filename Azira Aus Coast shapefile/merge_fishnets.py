import arcpy
import os

# Set up workspace
main_workspace = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Aus Coast Map.gdb"
arcpy.env.workspace = main_workspace
arcpy.env.overwriteOutput = True

def merge_existing_fishnets():
    """Merge all existing fishnet results from previous run"""
    try:
        print("Searching for existing fishnet layers...")
        
        # Get all feature classes that start with "Fishnet_Clipped_"
        arcpy.env.workspace = main_workspace
        all_features = arcpy.ListFeatureClasses()
        
        fishnet_layers = []
        for fc in all_features:
            if fc.startswith("Fishnet_Clipped_"):
                fishnet_layers.append(fc)
        
        print(f"Found {len(fishnet_layers)} existing fishnet layers")
        
        if len(fishnet_layers) == 0:
            print("No fishnet layers found. They may have been deleted or saved elsewhere.")
            return
        
        # Show first few for verification
        print("First 5 layers found:")
        for i, layer in enumerate(fishnet_layers[:5]):
            print(f"  {i+1}. {layer}")
        
        if len(fishnet_layers) > 5:
            print(f"  ... and {len(fishnet_layers) - 5} more")
        
        # Merge all layers
        merged_output = "Australia_Coastal_Fishnet_10m_Suburbs"
        print(f"\nMerging into: {merged_output}")
        
        arcpy.management.Merge(fishnet_layers, merged_output)
        
        # Verify the merge
        result = arcpy.management.GetCount(merged_output)
        total_features = int(result[0])
        print(f"Successfully merged {len(fishnet_layers)} layers")
        print(f"Total features in merged dataset: {total_features:,}")
        
        # Export to shapefile
        try:
            output_folder = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Output"
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
                
            shapefile_path = os.path.join(output_folder, f"{merged_output}.shp")
            arcpy.conversion.FeatureClassToShapefile(merged_output, output_folder)
            print(f"Also exported to shapefile: {shapefile_path}")
        except Exception as e:
            print(f"Warning: Could not export to shapefile: {str(e)}")
        
        print(f"\nMerged layer location: {os.path.join(main_workspace, merged_output)}")
        
    except Exception as e:
        print(f"Error during merge: {str(e)}")

if __name__ == "__main__":
    merge_existing_fishnets()