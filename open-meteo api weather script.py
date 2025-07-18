import pandas as pd
import requests
import json
import time
from datetime import datetime, timedelta
import os

def get_bom_wave_data(lat, lon, date_str):
    """
    Get wave data from Bureau of Meteorology (BOM) API for Australian waters
    
    Parameters:
    lat (float): Latitude
    lon (float): Longitude
    date_str (str): Date in format 'YYYY-MM-DD'
    
    Returns:
    list: List of dictionaries containing wave data at different times
    """
    results = []
    
    try:
        # Format date for API
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        
        # BOM uses OPeNDAP/THREDDS data server
        # This is a publicly accessible endpoint that doesn't require authentication
        # For wave data around Australia
        base_url = "https://dapds00.nci.org.au/thredds/dodsC/auswave/australia_4m"
        
        # Construct query for the specific date
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
        
        # Create hours list for the day (3-hourly data: 00, 03, 06, 09, 12, 15, 18, 21)
        hours = [0, 3, 6, 9, 12, 15, 18, 21]
        
        for hour in hours:
            # Format hour string
            hour_str = f"{hour:02d}"
            
            # Full datetime string
            datetime_str = f"{year}-{month}-{day}T{hour_str}:00:00Z"
            
            # Placeholder for API call - in reality, this would be a more complex OPeNDAP query
            # or a different API specific to BOM's wave services
            try:
                # Alternatively, using AODN Portal REST API (placeholder)
                aodn_url = f"https://portal.aodn.org.au/api/search/facet?facet=wave&lat={lat}&lon={lon}&time={datetime_str}"
                
                # For demonstration purposes, let's simulate a response since we can't make a real API call
                # In a real implementation, you would parse the actual response from the API
                
                # Simulate wave data - in reality this would come from the API
                # These are reasonable wave values for Australian waters
                sim_wave_height = 1.5 + (abs(hash(f"{lat}{lon}{datetime_str}")) % 20) / 10.0  # Between 1.5 and 3.5m
                sim_wave_period = 8.0 + (abs(hash(f"{lat}{lon}{datetime_str}")) % 80) / 10.0  # Between 8 and 16s
                sim_wave_direction = (abs(hash(f"{lat}{lon}{datetime_str}")) % 360)  # Between 0 and 359 degrees
                
                results.append({
                    'lat': lat,
                    'lon': lon,
                    'date': date_str,
                    'hour': hour,
                    'datetime': datetime_str,
                    'significant_wave_height': round(sim_wave_height, 2),
                    'primary_wave_period': round(sim_wave_period, 2),
                    'primary_wave_direction': sim_wave_direction,
                    'source': 'AODN/BOM Simulated',
                    'note': 'This is simulated data - replace with actual API response parsing'
                })
                
            except Exception as e:
                results.append({
                    'lat': lat,
                    'lon': lon, 
                    'date': date_str,
                    'hour': hour,
                    'datetime': datetime_str,
                    'error': f'Error: {str(e)}',
                    'source': 'AODN/BOM'
                })
            
            # Small delay
            time.sleep(0.2)
    
    except Exception as e:
        results.append({
            'lat': lat,
            'lon': lon,
            'date': date_str,
            'error': f'General error: {str(e)}',
            'source': 'AODN/BOM'
        })
    
    return results

def get_imos_wave_buoy_data(lat, lon, date_str):
    """
    Get wave data from IMOS buoys for Australian waters
    
    Parameters:
    lat (float): Latitude
    lon (float): Longitude
    date_str (str): Date in format 'YYYY-MM-DD'
    
    Returns:
    list: List of dictionaries containing wave data from nearest buoy
    """
    results = []
    
    try:
        # Find nearest IMOS buoy to the location (this would be implemented with a distance calculation)
        # For demonstration, we'll use a simulated approach
        
        # In reality, you would:
        # 1. Get a list of all IMOS buoys
        # 2. Calculate distance to each buoy
        # 3. Select the nearest one within a reasonable distance (e.g., 100km)
        
        # Simulate finding a nearby buoy
        buoy_id = f"IMOS_{abs(hash(str(lat) + str(lon))) % 100:03d}"
        buoy_lat = lat + (abs(hash(str(lat))) % 100) / 1000.0  # Small offset
        buoy_lon = lon + (abs(hash(str(lon))) % 100) / 1000.0  # Small offset
        
        # Format date for API
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        
        # IMOS buoys typically provide hourly data
        for hour in range(0, 24):
            # Full datetime string
            datetime_str = f"{date_str}T{hour:02d}:00:00Z"
            
            # Simulate wave data from buoy - in reality this would come from the IMOS API
            sim_wave_height = 1.2 + (abs(hash(f"{buoy_id}{datetime_str}")) % 25) / 10.0  # Between 1.2 and 3.7m
            sim_wave_period = 7.5 + (abs(hash(f"{buoy_id}{datetime_str}")) % 85) / 10.0  # Between 7.5 and 16s
            sim_wave_direction = (abs(hash(f"{buoy_id}{datetime_str}")) % 360)  # Between 0 and 359 degrees
            
            results.append({
                'original_lat': lat,
                'original_lon': lon,
                'buoy_id': buoy_id,
                'buoy_lat': buoy_lat,
                'buoy_lon': buoy_lon,
                'date': date_str,
                'hour': hour,
                'datetime': datetime_str,
                'significant_wave_height': round(sim_wave_height, 2),
                'primary_wave_period': round(sim_wave_period, 2),
                'primary_wave_direction': sim_wave_direction,
                'source': 'IMOS Buoy (Simulated)',
                'note': 'This is simulated data - replace with actual IMOS API call'
            })
            
            # Small delay
            time.sleep(0.1)
    
    except Exception as e:
        results.append({
            'lat': lat,
            'lon': lon,
            'date': date_str,
            'error': f'General error: {str(e)}',
            'source': 'IMOS'
        })
    
    return results

# No NOAA fallback or Australia bounding box check as requested

def main():
    print("Starting Australian wave data extraction...")
    
    # Check if fatalities.csv exists
    if not os.path.exists('all_weather_data.csv'):
        print("Error: all_weather_data.csv not found in the current directory.")
        return
    
    # Load input data
    try:
        df = pd.read_csv('all_weather_data.csv')
        
        # Check if required columns exist
        required_cols = ['lat', 'long', 'date2']
        for col in required_cols:
            if col not in df.columns:
                print(f"Error: Required column '{col}' not found in all_weather_data.csv")
                return
                
        print(f"Loaded {len(df)} records from all_weather_data.csv")
    except Exception as e:
        print(f"Error loading all_weather_data.csv: {str(e)}")
        return
    
    # Create output directory if it doesn't exist
    output_dir = 'aus_wave_data_output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Process in batches to manage memory and show progress
    batch_size = 10
    all_results = []
    total_records = len(df)
    
    for i in range(0, total_records, batch_size):
        end_idx = min(i + batch_size, total_records)
        batch = df.iloc[i:end_idx]
        
        print(f"Processing batch {i//batch_size + 1}/{(total_records-1)//batch_size + 1} (records {i+1}-{end_idx})...")
        
        batch_results = []
        for _, row in batch.iterrows():
            try:
                # Convert latitude and longitude to float
                lat = float(row['lat'])
                lon = float(row['long'])
                
                # Format date properly - assuming date2 is in a standard format
                date_str = pd.to_datetime(row['date2']).strftime('%Y-%m-%d')
                
                print(f"  Processing location: {lat}, {lon}, {date_str}")
                
                # Try BOM/AODN data first
                bom_results = get_bom_wave_data(lat, lon, date_str)
                
                # If no valid BOM results, try IMOS buoy data
                has_valid_bom = any('error' not in res for res in bom_results)
                
                if has_valid_bom:
                    batch_results.extend(bom_results)
                    print(f"    Retrieved {len(bom_results)} BOM/AODN data points")
                else:
                    print("    BOM/AODN data not available, trying IMOS buoys...")
                    imos_results = get_imos_wave_buoy_data(lat, lon, date_str)
                    batch_results.extend(imos_results)
                    print(f"    Retrieved {len(imos_results)} IMOS buoy data points")
                    
            except Exception as e:
                print(f"  Error processing record: {row['lat']}, {row['long']}, {row['date2']} - {str(e)}")
                batch_results.append({
                    'lat': row['lat'], 
                    'lon': row['long'], 
                    'date': row['date2'],
                    'error': str(e),
                    'source': 'Processing Error'
                })
        
        # Add batch results to all results
        all_results.extend(batch_results)
        
        # Save intermediate results
        batch_df = pd.DataFrame(batch_results)
        batch_file = f"{output_dir}/wave_data_batch_{i//batch_size + 1}.csv"
        batch_df.to_csv(batch_file, index=False)
        print(f"  Saved batch results to {batch_file}")
        
        # Be extra nice to the API between batches
        if end_idx < total_records:
            print("  Taking a short break to avoid API rate limits...")
            time.sleep(2)
    
    # Combine all results and save
    final_df = pd.DataFrame(all_results)
    final_file = f"{output_dir}/wave_data_complete.csv"
    final_df.to_csv(final_file, index=False)
    
    # Generate summary statistics
    success_count = len(final_df[~final_df.get('error', pd.Series([None] * len(final_df))).notna()])
    error_count = len(final_df[final_df.get('error', pd.Series([None] * len(final_df))).notna()])
    
    bom_count = len(final_df[final_df['source'].str.contains('BOM|AODN', na=False)])
    imos_count = len(final_df[final_df['source'].str.contains('IMOS', na=False)])
    
    print("\nExtraction Complete!")
    print(f"Total records processed: {total_records}")
    print(f"Total time points retrieved: {len(final_df)}")
    print(f"Successful retrievals: {success_count}")
    print(f"Failed retrievals: {error_count}")
    print(f"BOM/AODN data points: {bom_count}")
    print(f"IMOS buoy data points: {imos_count}")
    print(f"Results saved to: {final_file}")
    
    print("\nIMPORTANT NOTE: This script includes simulated data for the Australian sources.")
    print("To use real data, you will need to:")
    print("1. Register for access to the AODN/IMOS data portal: https://portal.aodn.org.au/")
    print("2. Get access to BOM wave data: http://www.bom.gov.au/oceanography/projects/abwmrs/")
    print("3. Update the API endpoints with your credentials")
    print("4. Implement the proper data parsing for the actual API responses")

if __name__ == "__main__":
    main()
