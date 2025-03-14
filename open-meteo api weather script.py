import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# FUNCTION: retrieves historical weather data from the Open-Meteo API using latitude, longitude, 
# and date information provided in a pandas DataFrame.

def get_historical_weather_from_dataframe(df):
    """
    Fetch historical weather data from a DataFrame with location/date data
    
    Parameters:
    df (pandas.DataFrame): DataFrame with latitude, longitude, and date columns
    
    Returns:
    dict: Dictionary with (lat, lon, date) tuples as keys and DataFrames as values
    pandas.DataFrame: Combined DataFrame with all weather data
    """
    
    # Check if required columns exist
    required_columns = ['latitude', 'longitude', 'date']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Missing required columns: {missing_columns}")
        print(f"Available columns: {df.columns.tolist()}")
        return None, None
    
    # Convert data to list of dictionaries
    # The orient='records' parameter specifically formats the output as a list of dictionaries where 
    # each dictionary represents a row in the DataFrame. This format is particularly well-suited for 
    # this function's task of iterating through location/date combinations.
    # For row-by-row processing, a list of dictionaries can be more efficient than repeatedly 
    # accessing the DataFrame, especially for larger datasets.
    locations_dates = df[required_columns].to_dict(orient='records')
    
    # Fetch weather data for each location/date
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    results = {} # an empty dictionary results to store the weather data
    
    # Loops through each location/date combination
    for i, item in enumerate(locations_dates):
        lat = item['latitude']
        lon = item['longitude']
        date = item['date']
        
        # Print progress
        print(f"Processing {i+1}/{len(locations_dates)}: {lat}, {lon}, {date}")
        
        # API parameters. Use the same date for both start and end to get a full day
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": date,
            "end_date": date,
            "hourly": "temperature_2m,relativehumidity_2m,precipitation,windspeed_10m,wind_direction_10m,pressure_msl,cloud_cover,wind_gusts_10m",
            "timezone": "Australia/Sydney",
            "temperature_unit": "celsius",
            "windspeed_unit": "kmh",
            "precipitation_unit": "mm"
        }
        
        # Opens a try block to catch any exceptions that might occur during the API request or data processing
        # important for handling network errors, API issues, or unexpected response formats without crashing the entire function
        try:
            response = requests.get(base_url, params=params) 
            # HTTP GET request to the Open-Meteo API, requests.get() is a function from the requests library that performs HTTP GET requests
            # function returns a Response object stored in the response variable

            if response.status_code == 200: # Checks if the response is successful (status code 200)
                data = response.json() # Parses the JSON response body into a Python dictionary
                # response.json() is a method that deserializes the JSON response content
                # makes the API response data accessible through Python dictionary syntax
                
                # Extract hourly data
                hourly_data = data["hourly"] # Extracts the "hourly" key from the parsed JSON data
                # The Open-Meteo API returns weather data in an "hourly" subsection of the response
                # This creates a reference to just that portion for easier access in the following code

                # Creates a new pandas DataFrame with renamed and formatted weather data
                weather_df = pd.DataFrame({
                    "datetime": pd.to_datetime(hourly_data["time"]), # pd.to_datetime() converts string time values to pandas datetime objects for better time handling
                    "temperature_celsius": hourly_data["temperature_2m"], # renames API's field names to more descriptive names
                    "humidity_percent": hourly_data["relativehumidity_2m"],
                    "precipitation_mm": hourly_data["precipitation"],
                    "wind_speed_kmh": hourly_data["windspeed_10m"]
                })
                
                # Sets the "datetime" column as the DataFrame's index
                weather_df.set_index("datetime", inplace=True) #inplace=True modifies existing df rather than creating a new one
                
                # Add location and date information to every row in the weather DataFrame
                weather_df['latitude'] = lat
                weather_df['longitude'] = lon
                weather_df['date'] = date
                
                # Add any extra columns from the input DataFrame for this specific row
                current_row = df[(df['latitude'] == lat) & (df['longitude'] == lon) & (df['date'] == date)]
                if not current_row.empty: # Checks if any matching rows were found in the original DataFrame, only proceeds if at least one match exists
                    for col in df.columns:
                        if col not in required_columns and col not in weather_df.columns: # checks column is not one of the required columns (latitude, longitude, date) that were already handled & that column doesn't already exist in the weather DataFrame
                            weather_df[col] = current_row[col].values[0] # Copies the value from the first matching row in the original DataFrame to every row in the weather DataFrame
                
                # Store in results dictionary with a tuple key
                key = (lat, lon, date)
                results[key] = weather_df
                
                # Add a small delay to avoid hitting rate limits
                time.sleep(0.5)
            else:
                print(f"Error for {lat}, {lon}, {date}: {response.status_code}")
                print(response.text)
        except Exception as e:
            print(f"Exception while processing {lat}, {lon}, {date}: {e}")
    
    # Combine all data into a single DataFrame
    if results:
        combined_data = pd.concat(results.values())
        return results, combined_data
    else:
        print("No weather data was retrieved")
        return {}, None

# Example usage
if __name__ == "__main__":

    input_csv = "beach_fatalities_geo.csv"
    df = pd.read_csv(input_csv)

    # Rename columns
    df = df.rename(columns={
        'lat': 'latitude',
        'long': 'longitude',
        'date2': 'date'
    })

    print(f"Successfully processed CSV: {df.shape[0]} rows, columns: {df.columns.tolist()}")

    # Call the function with the DataFrame
    # Assign the two return values from the function to two separate variables using tuple unpacking:
    # results_dict: Would receive the dictionary where keys are (lat, lon, date) tuples and values are individual DataFrames
    # all_data: Would receive the combined DataFrame containing all weather data
    results_dict, all_data = get_historical_weather_from_dataframe(df)

    if all_data is not None:
        # Create output directory if it doesn't exist
        output_dir = "weather_data"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save combined data to CSV
        all_data.to_csv(f"{output_dir}/all_weather_data.csv")
        all_data.to_csv("all_weather_data.csv")

        print(f"Saved all weather data to {output_dir}/all_weather_data.csv")
        
        # Optionally, save individual files
        for key, df in results_dict.items():
            lat, lon, date = key
            filename = f"{output_dir}/weather_{lat}_{lon}_{date}.csv"
            df.to_csv(filename)
            print(f"Saved {filename}")