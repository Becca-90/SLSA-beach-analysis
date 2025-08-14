import os
import shutil
from collections import defaultdict

def copy_half_gdb_files(source_gdb_path, dest_gdb_path):
    """
    Safely copy half of GDB files ensuring complete feature class groups
    """
    
    # First, create the destination GDB directory if it doesn't exist
    os.makedirs(dest_gdb_path, exist_ok=True)
    
    # Get all files in the source GDB
    all_files = [f for f in os.listdir(source_gdb_path) if os.path.isfile(os.path.join(source_gdb_path, f))]
    
    # Group files by their prefix (everything before the first dot)
    file_groups = defaultdict(list)
    system_files = []
    skipped_fishnet_files = []
    
    for file in all_files:
        # Skip files with "Fishnet_Clipped" in the name (already copied)
        if "Fishnet_Clipped" in file:
            skipped_fishnet_files.append(file)
            continue
            
        if '.' in file:
            prefix = file.split('.')[0]
            # Handle system files separately (these are usually single files)
            if prefix.lower() in ['gdb', 'timestamps', 'a00000001', 'a00000002', 'a00000003', 'a00000004']:
                system_files.append(file)
            else:
                file_groups[prefix].append(file)
        else:
            system_files.append(file)
    
    print(f"Found {len(file_groups)} feature class groups")
    print(f"Found {len(system_files)} system files")
    print(f"Skipped {len(skipped_fishnet_files)} Fishnet_Clipped files (already copied)")
    
    # Sort prefixes to ensure consistent splitting
    sorted_prefixes = sorted(file_groups.keys())
    
    # Take first half of the feature classes
    half_point = len(sorted_prefixes) // 2
    first_half_prefixes = sorted_prefixes[:half_point]
    
    print(f"Copying first {len(first_half_prefixes)} feature class groups (out of {len(sorted_prefixes)})")
    print(f"First group: {first_half_prefixes[0] if first_half_prefixes else 'None'}")
    print(f"Last group in first half: {first_half_prefixes[-1] if first_half_prefixes else 'None'}")
    
    # Files to copy
    files_to_copy = []
    
    # Add all system files (these are needed for GDB to function)
    files_to_copy.extend(system_files)
    
    # Add all files for the first half of feature classes
    for prefix in first_half_prefixes:
        files_to_copy.extend(file_groups[prefix])
        print(f"Group {prefix}: {len(file_groups[prefix])} files - {file_groups[prefix]}")
    
    print(f"\nTotal files to copy: {len(files_to_copy)}")
    
    # Copy the files
    copied_count = 0
    for file in files_to_copy:
        source_file = os.path.join(source_gdb_path, file)
        dest_file = os.path.join(dest_gdb_path, file)
        
        try:
            shutil.copy2(source_file, dest_file)
            copied_count += 1
            if copied_count % 100 == 0:
                print(f"Copied {copied_count} files...")
        except Exception as e:
            print(f"Error copying {file}: {e}")
    
    print(f"\nCopy complete! Copied {copied_count} files")
    print(f"Copied {len(first_half_prefixes)} complete feature class groups")
    print(f"Remaining {len(sorted_prefixes) - len(first_half_prefixes)} feature class groups left on external drive")

# Usage
if __name__ == "__main__":
    # CHANGE THESE PATHS TO YOUR ACTUAL PATHS
    source_path = r"E:\Aus Coast Map_ext.gdb"
    dest_path = r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\Aus Coast Map\Aus Coast Map_half1.gdb"
    
    print("Starting safe GDB file copy...")
    copy_half_gdb_files(source_path, dest_path)
    print("Done!")