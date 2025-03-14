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

def create_single_feature_class(output_path, data, fields_to_add):
    """Create a single feature class with all fields"""
        # Extract the output folder and file name
    out_path, out_name = os.path.split(output_path)

    print(f"Data received: {len(data)} rows")
    data.columns = data.columns.str.strip() #strip whitespace from column names
    
    # Reset index to ensure sequential ordering
    data = data.reset_index(drop=True)

    # Convert lat/lon to float before the loop
    data["Lat_S"] = pd.to_numeric(data["Lat_S"], errors="coerce").astype("float64")
    data["Long_E"] = pd.to_numeric(data["Long_E"], errors="coerce").astype("float64")

    # Create feature class using WGS84 (EPSG:4326)
    arcpy.management.CreateFeatureclass(
        os.path.dirname(output_path),
        os.path.basename(output_path),
        "POINT",
        spatial_reference=4326
    )
            
    # Add all fields
    for field_name, field_type in fields_to_add:
        arcpy.management.AddField(output_path, field_name, field_type, field_is_nullable="NULLABLE")

    print(f"Fields to be added: {fields_to_add}")
    print(f"DataFrame Columns: {data.columns.tolist()}")
                
    # Insert data
    fields = ["SHAPE@"] + [field[0] for field in fields_to_add]
    # Open ArcGIS InsertCursor
    with arcpy.da.InsertCursor(output_path, fields) as cursor:
        
        for idx, row in data.iterrows():
            print(f"Processing index {idx}")
            try:
                # define lat/lon
                lat = row["Lat_S"]  
                lon = row["Long_E"]

                # Skip invalid coordinates
                if pd.isna(lat) or pd.isna(lon):
                    print(f"Skipping row {idx}: Invalid coordinates")
                    continue

                point = arcpy.Point(lon, lat)
                point_geometry = arcpy.PointGeometry(point, arcpy.SpatialReference(4326))
                
                attributes = [point_geometry] + [
                    float(row.get(field[0], 0.0)) if field[1] == "DOUBLE" else
                    str(row.get(field[0], "Unknown"))
                    for field in fields_to_add if field[0] in data.columns
                ]

                print(f"Fields: {fields}")  # Ensure this matches attribute order
                print(f"Attributes: {attributes}")  # Ensure correct data is being inserted

                cursor.insertRow(attributes)
                    
            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                print(f"Full row data: {row}")
                continue

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
    project_name = "mapIncidentsallyearsfeb14test"  # You can easily change this variable
    
    fatalities2014_24_path = os.path.join(user_base_path, "Surf Life Saving Australia", "Lifesaving - Lifesaving", 
                                         "NCSR", "2024-2025 Season", "2024-25 Analyses", "Fatalities_2014-24_csv.csv")
    output_folder = os.path.join(user_base_path, "Surf Life Saving Australia", "SLSA Internal - Coastal Safety", 
                                "Research", "Python")
    output_shapefile = os.path.join(output_folder, "Fatalities_Complete.shp")

    # Initialize project
    aprx, map_obj = initialize_project(user_base_path, project_name)

    # Set column mapping
    column_name_mapping = {
        "Environment": "Env",
        "State": "State",
        "Incident.Time": "IncTime",
        "Incident.Date": "IncDate",
        "Incident_Season": "IncSeason",
        "Incident Type": "IncType",
        "Victim.Age": "VicAge",
        "Victim.Gender": "VicSex",
        "Incident_Location_Category": "Location",
        "Activity.Information": "Activity",
        "Suburb": "Suburb",
        "LGA": "LGA",
        "Remoteness": "Remoteness",
        "Incident.Month": "IncMonth",
        "Birth...Country": "BirthCtry",
        "Birth...Continent": "BirthCont",
        "Nationality": "Nationalit",
        "Residence.Distance.to.Coastline": "DistCoast",
        "Residence.Distance.to.Drowning.Location": "DistDrown",
        "Toxicology.Performed": "Toxicology",
        "Visitor.Category": "VisitorCat",
        "Distance.to.Lifesaving.Service": "DistSLS",
        "Rip.Present": "Rip",
        "Incident.Financial.Year": "FinYear",
        "GPS Latitude (S)": "Lat_S",
        "GPS Longitude (E)": "Long_E"
        }

    # Set data types
    data_type_mapping = {
        "Lat_S": "DOUBLE",
        "Long_E": "DOUBLE"
        }

    # print(df_wa.columns)
    # Read and prepare data
    df = pd.read_csv(fatalities2014_24_path, encoding='latin-1', skipinitialspace=True)
    df_wa = df[(df["State"] == "WA") & (df["Environment"] == "Coastal")]
    # Filter out rows with missing or "Unknown" coordinates
    df_wa = df_wa[
        (df_wa["GPS Latitude (S)"].notna()) & 
        (df_wa["GPS Longitude (E)"].notna()) & 
        (df_wa["GPS Latitude (S)"] != "Unknown") & 
        (df_wa["GPS Longitude (E)"] != "Unknown")
    ]
    print(f"Number of rows: {df_wa.shape[0]}")
    print(f"Number of columns: {df_wa.shape[1]}")

    df_wa.iloc[300][['GPS Latitude (S)', 'GPS Longitude (E)']]
    df_wa[['GPS Latitude (S)', 'GPS Longitude (E)']].dtypes

    # Add some validation
    print("\nChecking coordinates:")
    invalid_coords = df_wa[pd.isna(df_wa["GPS Latitude (S)"]) | pd.isna(df_wa["GPS Longitude (E)"])]
    if not invalid_coords.empty:
        print(f"\nFound {len(invalid_coords)} rows with invalid coordinates:")
        print(invalid_coords[["GPS Latitude (S)", "GPS Longitude (E)", "LGA", "Incident Date"]].head())
    else:
        print("All coordinates fine!")
    
    df_wa = df_wa.reset_index(drop=True)  # Ensures index starts at 0, 1, 2, ...

    df_wa_cols = {
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
        "Incident Financial Year": "FinYear",
        "GPS Latitude (S)": "Lat_S",
        "GPS Longitude (E)": "Long_E"
    }

    df_wa = df_wa.rename(columns=df_wa_cols)
    #print(df_wa.columns)

    # Filter df_wa to only include renamed columns
    filtered_columns = list(df_wa_cols.values())  # Keep only renamed columns
    df_wa = df_wa.filter(items=filtered_columns)
    #print(df_wa.columns)

    # Details of data
    print(len(df_wa.columns))
    print(f"Number of rows: {df_wa.shape[0]}")
    print(f"Number of columns: {df_wa.shape[1]}")
    print(df_wa.columns.tolist())
    print(df_wa.dtypes.to_string())

    # Generating fields_to_add
    fields_to_add = [
        (field, data_type_mapping.get(field, "TEXT"))  # Defaults to "TEXT" if field not in data_type_mapping
        for field in column_name_mapping.values()
    ]

    #check output
    print(fields_to_add)
    len(fields_to_add)
    len(df_wa)

    # Remove existing layers if they exist
    for layer in map_obj.listLayers():
        map_obj.removeLayer(layer)

    # Create single feature class with all attributes    
    create_single_feature_class(output_shapefile, df_wa, fields_to_add)

    # Add the newly created shapefile as a layer to the map
    map_obj.addDataFromPath(output_shapefile)
    
    #df.iloc[762]

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

    # Create summary by LGA (for WA only)
    lga_summary = df_wa.groupby('LGA').agg(
        Drowning=('IncType', lambda x: sum(x == "Drowning")),
        Death_unin=('IncType', lambda x: sum(x == "Death (unintentional)")),
        Death_int=('IncType', lambda x: sum(x == "Death (intentional)"))
    ).reset_index()

    suburb_summary = df_wa.groupby('Suburb').agg(
        Drowning=('IncType', lambda x: sum(x == "Drowning")),
        Death_unin=('IncType', lambda x: sum(x == "Death (unintentional)")),
        Death_int=('IncType', lambda x: sum(x == "Death (intentional)"))
    ).reset_index()

    suburb_summary[suburb_summary["Suburb"] == "Albany"]
    df_wa[df_wa["Suburb"] == "Albany"]


    # Add total column
    lga_summary['Drown_Fat'] = lga_summary['Death_unin'] + lga_summary['Drowning']
    suburb_summary['Drown_Fat'] = suburb_summary['Death_unin'] + suburb_summary['Drowning']

    # Flatten column names
    lga_summary.columns = ['LGA', 'Drowning', 'Death_unin', 'Death_int', 'Drown_Fat']
    suburb_summary.columns = ['Suburb', 'Drowning', 'Death_unin', 'Death_int', 'Drown_Fat']
    
    # Define output paths
    lga_summary_csv = os.path.join(output_folder, "LGA_Summary.csv")
    lga_summary_shp = os.path.join(output_folder, "LGA_Summary.shp")

    suburb_summary_csv = os.path.join(output_folder, "Suburb_Summary.csv")
    suburb_summary_shp = os.path.join(output_folder, "Suburb_Summary.shp")

    # Save to CSV first (as an intermediate step)
    lga_summary.to_csv(lga_summary_csv, index=False)
    suburb_summary.to_csv(suburb_summary_csv, index=False)

    # Get path to your LGA boundaries shapefile
    lga_boundaries = os.path.join(user_base_path, "Surf Life Saving Australia", "SLSA Internal - Coastal Safety", "Research", "Python", "LGAs, suburbs, etc", "LGA_2024_AUST_GDA2020_4326.shp")  # Replace with actual filename
    suburb_boundaries = os.path.join(user_base_path, "Surf Life Saving Australia", "SLSA Internal - Coastal Safety", "Research", "Python", "LGAs, suburbs, etc", "SAL_2021_AUST_GDA2020.shp")  # Replace with actual filename

    # Instead of creating a new empty feature class, we're joining the summary statistics to an existing LGA boundary shapefile
    # Create a feature layer from the LGA boundaries
    arcpy.management.MakeFeatureLayer(lga_boundaries, "lga_boundaries_layer")
    arcpy.management.MakeFeatureLayer(suburb_boundaries, "suburb_boundaries_layer")

    # Create a COPY of the LGA boundaries shapefile (prevents locking issues)
    lga_boundaries_copy = os.path.join(output_folder, "LGA_Boundaries_Copy.shp")
    arcpy.management.CopyFeatures(lga_boundaries, lga_boundaries_copy)

    suburb_boundaries_copy = os.path.join(output_folder, "suburb_Boundaries_Copy.shp")
    arcpy.management.CopyFeatures(suburb_boundaries, suburb_boundaries_copy)

    # 🔹 Use JoinField for a Permanent Join (instead of AddJoin)
    arcpy.management.JoinField(
        in_data=lga_boundaries_copy,
        in_field="LGA_NAME24",  # Ensure this matches the shapefile field
        join_table=lga_summary_csv,
        join_field="LGA",
        fields=["Drowning", "Death_unin", "Death_int", "Drown_Fat"]
    )

    arcpy.management.JoinField(
        in_data=suburb_boundaries_copy,
        in_field="SAL_NAME21",
        join_table=suburb_summary_csv,
        join_field="Suburb",
        fields=["Drowning", "Death_unin", "Death_int", "Drown_Fat"]
    )

    # Delete feature layer to release locks
    arcpy.management.Delete("lga_boundaries_layer")
    arcpy.management.Delete("suburb_boundaries_layer")

    # Save as final summary shapefile
    arcpy.management.CopyFeatures(lga_boundaries_copy, lga_summary_shp)
    arcpy.management.CopyFeatures(suburb_boundaries_copy, suburb_summary_shp)

    print("JoinField executed successfully on the copied dataset!")

    # Set first layer visible
    map_obj.listLayers()[0].visible = True
    map_obj.addBasemap("Oceans")

    # Add LGA summary layer
    summary_layer_name = "LGA Summary"
    summary_layer = arcpy.management.MakeFeatureLayer(lga_summary_shp, summary_layer_name)[0]
    map_layer = map_obj.addLayer(summary_layer)[0]

    summary_layer_name2 = "Suburb Summary"
    summary_layer2 = arcpy.management.MakeFeatureLayer(suburb_summary_shp, summary_layer_name2)[0]
    map_layer2 = map_obj.addLayer(summary_layer2)[0]

    # Apply graduated colors symbology based on total deaths
    symbology = map_layer.symbology
    symbology.updateRenderer('GraduatedColorsRenderer')
    symbology.renderer.field = "Tot_Deaths"

    symbology2 = map_layer2.symbology
    symbology2.updateRenderer('GraduatedColorsRenderer')
    symbology2.renderer.field = "Tot_Deaths"

    # Use a red color ramp for deaths
    color_ramps = aprx.listColorRamps("*Red*")
    if color_ramps:
        symbology.renderer.colorRamp = color_ramps[0]
    map_layer.symbology = symbology

    color_ramps2 = aprx.listColorRamps("*Red*")
    if color_ramps2:
        symbology2.renderer.colorRamp = color_ramps2[0]
    map_layer2.symbology = symbology2

    # Set first layer visible
    map_obj.listLayers()[0].visible = True

    # Add main layer and create symbology layers
    key_fields = ["Location", "Env", "FinYear", "IncSeason"]
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