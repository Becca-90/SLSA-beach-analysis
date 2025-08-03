import arcpy

# ULTRA FAST - hardcode everything
arcpy.env.overwriteOutput = True

# GDB paths
current_gdb = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Aus Coast Map.gdb"
external_gdb = r"E:\Aus Coast Map.gdb" 

# Build list of all fishnet feature classes
fishnets = []

# Add from current GDB
arcpy.env.workspace = current_gdb
current = arcpy.ListFeatureClasses("Fishnet_Clipped_Tile_*") or []
fishnets.extend([f"{current_gdb}\\{fc}" for fc in current])

# Add from external GDB  
arcpy.env.workspace = external_gdb
external = arcpy.ListFeatureClasses("Fishnet_Clipped_Tile_*") or []
fishnets.extend([f"{external_gdb}\\{fc}" for fc in external])

print(f"Merging {len(fishnets)} fishnets...")

# DO THE MERGE
arcpy.env.workspace = current_gdb
arcpy.management.Merge(fishnets, "MERGED_FISHNETS")

print("DONE!")