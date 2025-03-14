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
    project_name = "mapIncidentsallyearsmarch1test"
    
    fatalities2014_24_path = os.path.join(user_base_path, "Surf Life Saving Australia", "Lifesaving - Lifesaving", 
                                         "NCSR", "2024-2025 Season", "2024-25 Analyses", "Fatalities_2014-24_csv.csv")
    output_folder = os.path.join(user_base_path, "Surf Life Saving Australia", "SLSA Internal - Coastal Safety", 
                                "Research", "Python")
    output_shapefile = os.path.join(output_folder, "Fatalities_Complete.shp")

    # Get boundary shapefiles 
    lga_boundaries = os.path.join(user_base_path, "Surf Life Saving Australia", "SLSA Internal - Coastal Safety", 
                                "Research", "Python", "LGAs, suburbs, etc", "LGA_2024_AUST_GDA2020_4326.shp")
    suburb_boundaries = os.path.join(user_base_path, "Surf Life Saving Australia", "SLSA Internal - Coastal Safety", 
                                   "Research", "Python", "LGAs, suburbs, etc", "SAL_2021_AUST_GDA2020.shp")

    # Initialize project
    aprx, map_obj = initialize_project(user_base_path, project_name)

    # Set data types
    data_type_mapping = {
        "Lat_S": "DOUBLE",
        "Long_E": "DOUBLE"
    }

    # Read and prepare data
    df = pd.read_csv(fatalities2014_24_path, encoding='latin-1', skipinitialspace=True)
    df_wa = df[(df["State"] == "WA") & (df["Environment"] == "Coastal")]
    
    # Filter out rows with missing or "Unknown" coordinates
    df_wa = df_wa[
        (df_wa["GPS Latitude (S)"].notna()) & 
        (df_wa["GPS Longitude (E)"].notna()) & 
        (df_wa["GPS Latitude (S)"] != "Unknown") & 
        (df_wa["GPS Longitude (E)"] != "Unknown") &
        (df_wa["GPS Latitude (S)"] != "unknown") & 
        (df_wa["GPS Longitude (E)"] != "unknown")        
    ]
    
    df_wa = df_wa.reset_index(drop=True)  # Ensures index starts at 0, 1, 2, ...

    # Column renaming
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
    
    # Filter df_wa to only include renamed columns
    filtered_columns = list(df_wa_cols.values())
    df_wa = df_wa.filter(items=filtered_columns)
    
    # Generating fields_to_add for shapefile
    fields_to_add = [
        (field, data_type_mapping.get(field, "TEXT"))
        for field in df_wa_cols.values()
    ]

    # Remove existing layers if they exist
    for layer in map_obj.listLayers():
        map_obj.removeLayer(layer)

    # Create single feature class with all attributes    
    create_single_feature_class(output_shapefile, df_wa, fields_to_add)

    # Add the newly created shapefile as a layer to the map
    map_obj.addDataFromPath(output_shapefile)

    # Convert to GeoDataFrame
    fatalities_gdf = geopandas.GeoDataFrame(df_wa, 
                                      geometry=geopandas.points_from_xy(df_wa["Long_E"], df_wa["Lat_S"]), 
                                      crs="EPSG:4326")  # Ensure correct coordinate reference system

    # Load boundary data as GeoDataFrames
    lga_boundaries = geopandas.read_file(lga_boundaries)
    suburb_boundaries = geopandas.read_file(suburb_boundaries)

    # Perform Spatial Joins to determine correct LGA and Suburb
    fatalities_gdf = geopandas.sjoin(fatalities_gdf, lga_boundaries[['LGA_NAME24', 'geometry']], how="left", predicate="within")
    fatalities_gdf = fatalities_gdf.rename(columns={'LGA_NAME24': 'LGA'})

    fatalities_gdf = geopandas.sjoin(fatalities_gdf, suburb_boundaries[['SAL_NAME21', 'geometry']], how="left", predicate="within")
    fatalities_gdf = fatalities_gdf.rename(columns={'SAL_NAME21': 'Suburb'})

    # Remove the old Suburb and LGA columns (they were incorrect)
    fatalities_gdf.drop(columns=['index_right'], errors='ignore', inplace=True)

    # Save the updated fatalities dataset with correct LGA and Suburb
    fatalities_gdf.to_file(output_shapefile, driver="ESRI Shapefile")

    # Create summary by LGA
    lga_summary = fatalities_gdf.groupby('LGA').agg(
        Drowning=('Incident Type', lambda x: sum(x == "Drowning")),
        Death_unin=('Incident Type', lambda x: sum(x == "Death (unintentional)")),
        Death_int=('Incident Type', lambda x: sum(x == "Death (intentional)"))
    ).reset_index()

    # Create summary by Suburb
    suburb_summary = fatalities_gdf.groupby('Suburb').agg(
        Drowning=('Incident Type', lambda x: sum(x == "Drowning")),
        Death_unin=('Incident Type', lambda x: sum(x == "Death (unintentional)")),
        Death_int=('Incident Type', lambda x: sum(x == "Death (intentional)"))
    ).reset_index()

    # Add total column
    lga_summary['Drown_Fat'] = lga_summary['Death_unin'] + lga_summary['Drowning']
    suburb_summary['Drown_Fat'] = suburb_summary['Death_unin'] + suburb_summary['Drowning']

    # Save summary tables
    lga_summary_csv = os.path.join(output_folder, "LGA_Summary.csv")
    suburb_summary_csv = os.path.join(output_folder, "Suburb_Summary.csv")
    
    lga_summary.to_csv(lga_summary_csv, index=False)
    suburb_summary.to_csv(suburb_summary_csv, index=False)

    # Join summary data back to boundaries for visualization
    lga_boundaries_copy = os.path.join(output_folder, "LGA_Boundaries_Copy.shp")
    suburb_boundaries_copy = os.path.join(output_folder, "Suburb_Boundaries_Copy.shp")

    arcpy.management.CopyFeatures(lga_boundaries, lga_boundaries_copy)
    arcpy.management.CopyFeatures(suburb_boundaries, suburb_boundaries_copy)

    arcpy.management.JoinField(
        in_data=lga_boundaries_copy,
        in_field="LGA_NAME24",
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

    # Save as final summary shapefiles
    lga_summary_shp = os.path.join(output_folder, "LGA_Summary.shp")
    suburb_summary_shp = os.path.join(output_folder, "Suburb_Summary.shp")

    arcpy.management.CopyFeatures(lga_boundaries_copy, lga_summary_shp)
    arcpy.management.CopyFeatures(suburb_boundaries_copy, suburb_summary_shp)

    # Add layers to the map
    aprx, map_obj = initialize_project(user_base_path, project_name)
    map_obj.addDataFromPath(lga_summary_shp)
    map_obj.addDataFromPath(suburb_summary_shp)
    
    # Add basemap
    map_obj.addBasemap("Oceans")

    # Save the project
    aprx.save()
    print("Project saved successfully with simplified layers.")

if __name__ == "__main__":
    main()