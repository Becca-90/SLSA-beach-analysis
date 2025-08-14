import arcpy
import time

def merge_fishnets_in_batches(gdb_path, batch_size=5000):
    """
    Merge fishnet tiles in manageable batches to avoid memory/performance issues
    """
    
    arcpy.env.overwriteOutput = True
    arcpy.env.workspace = gdb_path
    
    print("Getting fishnet list from external GDB...")
    start_time = time.time()
    
    # Get all fishnets (this will still take time, but only once)
    all_fishnets = arcpy.ListFeatureClasses("Fishnet_Clipped_Tile_*") or []
    
    scan_time = time.time() - start_time
    print(f"Found {len(all_fishnets)} fishnets in {scan_time/60:.1f} minutes")
    
    if not all_fishnets:
        print("No fishnets found!")
        return
    
    # Process in batches
    batch_outputs = []
    total_batches = (len(all_fishnets) + batch_size - 1) // batch_size  # Ceiling division
    
    print(f"Processing in {total_batches} batches of ~{batch_size} files each...")
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, len(all_fishnets))
        
        batch_fishnets = all_fishnets[start_idx:end_idx]
        batch_output = f"BATCH_{batch_num+1:02d}_MERGED"
        
        print(f"\nBatch {batch_num+1}/{total_batches}: Merging {len(batch_fishnets)} fishnets...")
        print(f"  From: {batch_fishnets[0]}")
        print(f"  To: {batch_fishnets[-1]}")
        
        try:
            batch_start = time.time()
            arcpy.management.Merge(batch_fishnets, batch_output)
            batch_time = time.time() - batch_start
            
            # Get count for this batch
            count = int(arcpy.management.GetCount(batch_output)[0])
            print(f"  ✓ Batch {batch_num+1} complete: {count:,} features in {batch_time/60:.1f} minutes")
            
            batch_outputs.append(batch_output)
            
        except Exception as e:
            print(f"  ✗ Error in batch {batch_num+1}: {e}")
            continue
    
    # Final merge of all batches
    if len(batch_outputs) > 1:
        print(f"\nFinal step: Merging {len(batch_outputs)} batches into final output...")
        try:
            final_start = time.time()
            arcpy.management.Merge(batch_outputs, "FINAL_MERGED_ALL_FISHNETS")
            final_time = time.time() - final_start
            
            final_count = int(arcpy.management.GetCount("FINAL_MERGED_ALL_FISHNETS")[0])
            total_time = (time.time() - start_time) / 60
            
            print(f"🎉 SUCCESS! Final merge complete:")
            print(f"   Total features: {final_count:,}")
            print(f"   Total time: {total_time:.1f} minutes")
            print(f"   Output: FINAL_MERGED_ALL_FISHNETS")
            
            # Clean up batch files to save space
            cleanup = input("\nDelete intermediate batch files? (y/n): ")
            if cleanup.lower() == 'y':
                for batch in batch_outputs:
                    try:
                        arcpy.management.Delete(batch)
                        print(f"  Deleted {batch}")
                    except:
                        print(f"  Could not delete {batch}")
                        
        except Exception as e:
            print(f"✗ Error in final merge: {e}")
            print("Batch files are available for manual merging")
            
    elif len(batch_outputs) == 1:
        # Only one batch - rename it
        arcpy.management.Rename(batch_outputs[0], "FINAL_MERGED_ALL_FISHNETS")
        final_count = int(arcpy.management.GetCount("FINAL_MERGED_ALL_FISHNETS")[0])
        print(f"🎉 Single batch complete: {final_count:,} features")
    
    print("Done!")

# Usage
if __name__ == "__main__":
    # CHANGE THIS PATH TO YOUR EXTERNAL GDB
    gdb_path = r"E:\Aus Coast Map_ext.gdb"
    
    print("Starting batch merge of fishnets...")
    print("This will work directly on your external drive.")
    
    # You can adjust batch_size based on your system:
    # - 5000 = safer, more batches
    # - 10000 = fewer batches, more memory usage  
    # - 2000 = very safe for slow systems
    
    merge_fishnets_in_batches(gdb_path, batch_size=5000)