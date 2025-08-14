import arcpy

# SIMPLE - CURRENT GDB ONLY
arcpy.env.overwriteOutput = True
arcpy.env.workspace = r"E:\Aus Coast Map_ext.gdb"

print("Getting fishnets from external GDB only...")
fishnets = arcpy.ListFeatureClasses("Fishnet_Clipped_Tile_*") or []
print(f"Found {len(fishnets)} fishnets")

if fishnets:
    print("Merging...")
    arcpy.management.Merge(fishnets, "MERGED_EXTERNAL_ONLY")
    count = int(arcpy.management.GetCount("MERGED_EXTERNAL_ONLY")[0])
    print(f"DONE! {count:,} features")
else:
    print("No fishnets found")