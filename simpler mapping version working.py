import os
import pandas as pd
import arcpy
import geopandas

arcpy.env.overwriteOutput = True

def apply_unique_value_symbology(layer, field_name):
    symbology = layer.symbology
    symbology.updateRenderer('UniqueValueRenderer')
    symbology.renderer.fields = [field_name]
    
    aprx = arcpy.mp.ArcGISProject(r"C:\Users\RebeccaStolper\Documents\ArcGIS\Projects\mapIncidents10years\mapIncidents10years.aprx")
    color_ramps = aprx.listColorRamps()
    if color_ramps:
        symbology.renderer.colorRamp = color_ramps[0]
    
    layer.symbology = symbology
    return layer

def create_single_feature_class(output_path, df, fields_to_add):
    """Create a single feature class with all fields"""
    # Create feature class
    arcpy.management.CreateFeatureclass(
        os.path.dirname(output_path),
        os.path.basename(output_path),
        "POINT",
        spatial_reference=4326
    )
    
    # Add all fields
    for field_name, field_type in fields_to_add:
        arcpy.management.AddField(output_path, field_name, field_type, field_is_nullable="NULLABLE")
    
    # Insert data
    fields = ["SHAPE@"] + [field[0] for field in fields_to_add]
    with arcpy.da.InsertCursor(output_path, fields) as cursor:
        for _, row in df.iterrows():
            lat = pd.to_numeric(row.get("GPS Latitude (S)", None), errors="coerce")
            lon = pd.to_numeric(row.get("GPS Longitude (E)", None), errors="coerce")
            
            if pd.isna(lat) or pd.isna(lon):
                continue
                
            point = arcpy.Point(lon, lat)
            attributes = [point] + [str(row.get(field[0], "Unknown")) for field in fields_to_add]
            cursor.insertRow(attributes)

"""def reproject_shapefile(shapefile_path):
    Reproject shapefile to EPSG:4326 if needed
    gdf = geopandas.read_file(shapefile_path)
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
        output_path = shapefile_path.replace('.shp', '_4326.shp')
        gdf.to_file(output_path)
        return output_path
    return shapefile_path
"""

def reproject_shapefile(shapefile_path):
    """
    Reproject shapefile to EPSG:4326 if needed.
    Ensures existing reprojected files are replaced and avoids duplicate suffixes.

    Args:
        shapefile_path (str): Path to the original shapefile.

    Returns:
        str: Path to the reprojected shapefile.
    """
    gdf = geopandas.read_file(shapefile_path)

    # Check if reprojection is necessary
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        # Generate the reprojected file path
        base_name, ext = os.path.splitext(shapefile_path)
        if base_name.endswith("_4326"):
            output_path = shapefile_path  # Already projected file name
        else:
            output_path = f"{base_name}_4326{ext}"

        # Delete existing reprojected file, if it exists
        if os.path.exists(output_path):
            os.remove(output_path)

        # Reproject to EPSG:4326 and save
        gdf = gdf.to_crs(epsg=4326)
        gdf.to_file(output_path)
        print(f"Reprojected: {output_path}")
        return output_path

    # If no reprojection needed, return original path
    return shapefile_path

def main():
    # Set up paths
    user_base_path = r"C:\Users\RebeccaStolper"
    fatalities2014_24_path = os.path.join(user_base_path, "Surf Life Saving Australia", "Lifesaving - Lifesaving", 
                                         "NCSR", "2024-2025 Season", "2024-25 Analyses", "Fatalities_2014-24.csv")
    output_folder = os.path.join(user_base_path, "Surf Life Saving Australia", "SLSA Internal - Coastal Safety", 
                                "Research", "Python")
    output_shapefile = os.path.join(output_folder, "Fatalities_Complete.shp")

    # Column mapping
    column_name_mapping = {
        "Environment": "Env",
        "State": "State",
        "Incident Time": "IncTime",
        "Incident Date": "IncDate",
        "Incident_Season": "IncSeason",
        "Incident Type": "IncType",
        "Victim Age": "VicAge",
        "Victim Gender": "VicSex",
        "Incident_Location_Category": "Location",
        "Activity Information": "Activity",
        "Suburb": "Suburb",
        "LGA": "LGA",
        "Remoteness": "Remoteness",
        "Incident Month": "IncMonth",
        "Birth - Country": "BirthCtry",
        "Birth - Continent": "BirthCont",
        "Nationality": "Nationalit",
        "Residence Distance to Coastline": "DistCoast",
        "Residence Distance to Drowning Location": "DistDrown",
        "Toxicology Performed": "Toxicology",
        "Visitor Category": "VisitorCat",
        "Distance to Lifesaving Service": "DistSLS",
        "Rip Present": "Rip",
        "Incident Financial Year": "FinYear"
    }

    # Read and prepare data
    df = pd.read_csv(fatalities2014_24_path, encoding='latin-1')
    df.rename(columns=column_name_mapping, inplace=True)
    df.fillna("Unknown", inplace=True)

    # Define fields
    fields_to_add = [(field, "TEXT") for field in column_name_mapping.values()]

    # Create single feature class with all attributes
    create_single_feature_class(output_shapefile, df, fields_to_add)

    # Add to ArcGIS Pro project
    aprx = arcpy.mp.ArcGISProject(os.path.join(user_base_path, "Documents", "ArcGIS", 
                                              "Projects", "mapIncidents10years", "mapIncidents10years.aprx"))
    map_obj = aprx.listMaps()[0]

    # Remove existing layers if they exist
    for layer in map_obj.listLayers():
        map_obj.removeLayer(layer)

    # Add main layer and create symbology layers
    key_fields = ["Location", "Env", "FinYear", "IncSeason"]
    group_layer = map_obj.addLayer(arcpy.management.MakeFeatureLayer(output_shapefile, "Fatalities")[0])[0]
    
    # Create a group layer for different views
    for field in key_fields:
        layer_name = f"Fatalities by {field}"
        layer_view = arcpy.management.MakeFeatureLayer(output_shapefile, layer_name)[0]
        map_layer = map_obj.addLayer(layer_view)[0]
        apply_unique_value_symbology(map_layer, field)
        map_layer.visible = False  # Set all to invisible initially
    
    # Set first layer visible
    map_obj.listLayers()[0].visible = True

    # Add LGA and suburb shapefiles
    lga_folder = os.path.join(output_folder, "LGAs, suburbs, etc")
    if os.path.exists(lga_folder):
        processed_files = set()  # Keep track of processed files
        for file in os.listdir(lga_folder):
            if file.endswith(".shp") and not file.endswith(("_4326.shp", "_reproj.shp")):
                base_name = os.path.splitext(file)[0]
                if base_name not in processed_files:
                    shapefile_path = os.path.join(lga_folder, file)
                    reprojected_path = reproject_shapefile(shapefile_path)
                    
                    try:
                        layer = map_obj.addDataFromPath(reprojected_path)
                        layer.name = f"Reference: {base_name}"

                        # Apply unique value symbology to RA shapefile
                        if "RA_2021" in base_name:
                            apply_unique_value_symbology(layer, "RA_NAME21")  # Field containing remoteness categories                    

                        processed_files.add(base_name)
                    except Exception as e:
                        print(f"Failed to add {file}: {e}")

    aprx.save()
    print("Project saved successfully with organized layers.")

if __name__ == "__main__":
    main()