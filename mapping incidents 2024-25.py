import os
import pandas as pd
import arcpy
import geopandas

arcpy.env.overwriteOutput = True

def initialize_project(user_base_path, project_name):
    """Initialize or create ArcGIS project and folder"""
    # Create project folder path
    project_folder = os.path.join(user_base_path, "Documents", "ArcGIS", "Projects", project_name)
    project_path = os.path.join(project_folder, f"{project_name}.aprx")

    # Create project folder if it doesn't exist
    if not os.path.exists(project_folder):
        os.makedirs(project_folder)
        print(f"Created project folder: {project_folder}")
    
    # Create or open project
    if not os.path.exists(project_path):
        print(f"Creating new ArcGIS Pro project: {project_path}")
        # Create new project from blank template
        template_path = r"C:\Program Files\ArcGIS\Pro\Resources\ArcToolBox\Services\routingservices\data\Blank.aprx"
        aprx = arcpy.mp.ArcGISProject(template_path)
        aprx.saveACopy(project_path)
        # Open the new project
        aprx = arcpy.mp.ArcGISProject(project_path)
    else:
        print(f"Opening existing project: {project_path}")
        aprx = arcpy.mp.ArcGISProject(project_path)

    # Ensure project has at least one map and add basemap
    if not aprx.listMaps():
        map_obj = aprx.createMap("Map")
        # Add World Basemap
        map_obj.addBasemap("Oceans")
        aprx.save()
    else:
        map_obj = aprx.listMaps()[0]
        # Check if basemap exists, if not add it
        has_basemap = False
        for layer in map_obj.listLayers():
            if layer.isBasemapLayer:
                has_basemap = True
                break
        if not has_basemap:
            map_obj.addBasemap("Oceans")
    
    return aprx, map_obj

def apply_unique_value_symbology(layer, field_name, aprx):
    symbology = layer.symbology
    symbology.updateRenderer('UniqueValueRenderer')
    symbology.renderer.fields = [field_name]
    
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

def reproject_shapefile(shapefile_path):
    """
    Reproject shapefile to EPSG:4326 if needed.
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

    return shapefile_path

def main():
    # Set up paths and project name
    user_base_path = r"C:\Users\RebeccaStolper"
    project_name = "mapIncidents23-24"  # You can easily change this variable
    
    fatalities2014_24_path = os.path.join(user_base_path, "Surf Life Saving Australia", "Lifesaving - Lifesaving", 
                                         "NCSR", "2024-2025 Season", "2024-25 Analyses", "Fatalities_2014-24.csv")
    output_folder = os.path.join(user_base_path, "Surf Life Saving Australia", "SLSA Internal - Coastal Safety", 
                                "Research", "Python")
    output_shapefile = os.path.join(output_folder, "Fatalities_Complete.shp")

    # Initialize project
    aprx, map_obj = initialize_project(user_base_path, project_name)

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

    # Filter for Financial Year
    df = df[df["FinYear"] == "2023-24"]
    print(f"Filtered records count: {len(df)}")

    # Define fields
    fields_to_add = [(field, "TEXT") for field in column_name_mapping.values()]
    
    # Create single feature class with all attributes
    create_single_feature_class(output_shapefile, df, fields_to_add)

    # Remove existing layers if they exist
    for layer in map_obj.listLayers():
        map_obj.removeLayer(layer)

    # Add LGA and suburb shapefiles
    lga_folder = os.path.join(output_folder, "LGAs, suburbs, etc")
    if os.path.exists(lga_folder):
        processed_files = set()
        for file in os.listdir(lga_folder):
            if file.endswith(".shp") and not file.endswith(("_4326.shp", "_reproj.shp")):
                base_name = os.path.splitext(file)[0]
                if base_name not in processed_files:
                    shapefile_path = os.path.join(lga_folder, file)
                    reprojected_path = reproject_shapefile(shapefile_path)
                    
                    try:
                        layer = map_obj.addDataFromPath(reprojected_path)
                        layer.name = f"Reference: {base_name}"

                        if "RA_2021" in base_name:
                            apply_unique_value_symbology(layer, "RA_NAME21", aprx)  # Pass aprx to the function

                        processed_files.add(base_name)
                    except Exception as e:
                        print(f"Failed to add {file}: {e}")
    
    # Create summary by LGA
    #lga_summary = df.groupby('LGA').agg({
    #    'IncType': [
    #        ('Drowning', lambda x: sum(x == "Drowning")),
    #        ('Death_unin', lambda x: sum(x == "Death (unintentional)")),
    #        ('Death_int', lambda x: sum(x == "Death (intentional)"))
    #    ]
    #}).reset_index()

    # Create summary by LGA
    lga_summary = df.groupby('LGA').agg(
        Drowning=('IncType', lambda x: sum(x == "Drowning")),
        Death_unin=('IncType', lambda x: sum(x == "Death (unintentional)")),
        Death_int=('IncType', lambda x: sum(x == "Death (intentional)"))
    ).reset_index()

    # Add total column
    lga_summary['Tot_Deaths'] = lga_summary['Death_unin'] + lga_summary['Death_int']

    # Flatten column names
    lga_summary.columns = ['LGA', 'Drowning', 'Death_unin', 'Death_int', 'Tot_Deaths']

    # Define output paths
    fatalities_summary_csv = os.path.join(output_folder, "LGA_Summary.csv")
    summary_shapefile = os.path.join(output_folder, "LGA_Summary.shp")

    # Save to CSV first (as intermediate step)
    lga_summary.to_csv(fatalities_summary_csv, index=False)

    # Get path to your LGA boundaries shapefile
    lga_boundaries = os.path.join(user_base_path, "Surf Life Saving Australia", "SLSA Internal - Coastal Safety", "Research", "Python", "LGAs, suburbs, etc", "LGA_2024_AUST_GDA2020_4326.shp")  # Replace with actual filename

    # Instead of creating a new empty feature class, we're joining the summary statistics to an existing LGA boundary shapefile
    # Create a feature layer from the LGA boundaries
    arcpy.management.MakeFeatureLayer(lga_boundaries, "lga_boundaries_layer")

    # Create a COPY of the LGA boundaries shapefile (prevents locking issues)
    lga_boundaries_copy = os.path.join(output_folder, "LGA_Boundaries_Copy.shp")
    arcpy.management.CopyFeatures(lga_boundaries, lga_boundaries_copy)

    # 🔹 Use JoinField for a Permanent Join (instead of AddJoin)
    arcpy.management.JoinField(
        in_data=lga_boundaries_copy,
        in_field="LGA_NAME24",  # Ensure this matches the shapefile field
        join_table=fatalities_summary_csv,
        join_field="LGA",
        fields=["Drowning", "Death_unin", "Death_int", "Tot_Deaths"]
    )

    # Delete feature layer to release locks
    arcpy.management.Delete("lga_boundaries_layer")

    # Save as final summary shapefile
    arcpy.management.CopyFeatures(lga_boundaries_copy, summary_shapefile)

    print("JoinField executed successfully on the copied dataset!")

    # Set first layer visible
    map_obj.listLayers()[0].visible = True
    map_obj.addBasemap("Oceans")

    # Add LGA summary layer
    summary_layer_name = "LGA Summary"
    summary_layer = arcpy.management.MakeFeatureLayer(summary_shapefile, summary_layer_name)[0]
    map_layer = map_obj.addLayer(summary_layer)[0]

    # Apply graduated colors symbology based on total deaths
    symbology = map_layer.symbology
    symbology.updateRenderer('GraduatedColorsRenderer')
    symbology.renderer.field = "Tot_Deaths"
    # Use a red color ramp for deaths
    color_ramps = aprx.listColorRamps("*Red*")
    if color_ramps:
        symbology.renderer.colorRamp = color_ramps[0]
    map_layer.symbology = symbology

    # Make the layer visible
    map_layer.visible = True

    # Add main layer and create symbology layers
    key_fields = ["Location", "Env", "IncSeason"]
    group_layer = map_obj.addLayer(arcpy.management.MakeFeatureLayer(output_shapefile, "Fatalities")[0])[0]
    
    # Create a group layer for different views
    for field in key_fields:
        layer_name = f"Fatalities by {field}"
        layer_view = arcpy.management.MakeFeatureLayer(output_shapefile, layer_name)[0]
        map_layer = map_obj.addLayer(layer_view)[0]
        apply_unique_value_symbology(map_layer, field, aprx)  # Pass aprx to the function
        map_layer.visible = False

    try:
        aprx.save()
        print("Project saved successfully with organized layers.")
    finally:
        del aprx

if __name__ == "__main__":
    main()        