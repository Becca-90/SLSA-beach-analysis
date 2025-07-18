import requests
import pandas as pd
from io import StringIO

def pointdata(variables,
              username,
              password=None,
              start=None,
              finish=None,
              station=None,
              lat=None,
              lon=None,
              units=True,
              output=None):

    import requests
    import pandas as pd
    from io import StringIO

    unit_defs = {
        'daily_rain': 'mm',
        'max_temp': 'Celsius',
        'min_temp': 'Celsius',
        'vp': 'hPa',
        'vp_deficit': 'hPa',
        'evap_pan': 'mm',
        'evap_syn': 'mm',
        'evap_comb': 'mm',
        'evap_morton_lake': 'mm',
        'radiation': 'MJm-2',
        'rh_tmax': '%',
        'rh_tmin': '%',
        'et_short_crop': 'mm',
        'et_tall_crop': 'mm',
        'et_morton_actual': 'mm',
        'et_morton_potential': 'mm',
        'et_morton_wet': 'mm',
        'mslp': 'hPa',
    }

    # Validate inputs
    if station is None and (lat is None or lon is None):
        raise ValueError("'lat' and 'lon' must be provided if 'station' is not specified.")

    # Basic API parameters
    params = {
        'username': username,
        'format': 'csv',
        'dataset': 'Official',
    }

    # Convert variables to API format (comma-separated)
    if isinstance(variables, list):
        params['variables'] = ','.join(variables)
    else:
        params['variables'] = variables

    # Convert start and finish dates to `YYYYMMDD` format
    if start:
        params['start'] = start.strftime('%Y%m%d') if hasattr(start, 'strftime') else str(start)
    if finish:
        params['finish'] = finish.strftime('%Y%m%d') if hasattr(finish, 'strftime') else str(finish)

    # Add password if provided
    if password:
        params['password'] = password

    # Remove output param if None
    params.pop('output', None)

    # Determine API endpoint
    if station:
        base_url = 'https://www.longpaddock.qld.gov.au/cgi-bin/silo/PatchedPointDataset.php'
        params['station'] = station
    else:
        base_url = 'https://www.longpaddock.qld.gov.au/cgi-bin/silo/DataDrillDataset.php'
        params['lat'] = lat
        params['lon'] = lon

    try:
        r = requests.get(base_url, params=params, timeout=30)
        r.raise_for_status()
        
        text = r.content.decode()

        # Print API response for debugging
        print("API Response first 200 characters:")
        print(text[:200])

        # Check if API returned an error
        if "Error" in text or "need variables for json or csv format" in text:
            print("API Error Response:")
            print(text)
            return None

        # Read CSV response
        df = pd.read_csv(StringIO(text))

        # Rename date column if found
        if 'YYYY-MM-DD' in df.columns:
            df = df.rename(columns={'YYYY-MM-DD': 'date'})

        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')

        # Append units to column names
        if units:
            labels = {key: f"{key}_{unit_defs[key]}" for key in df.columns if key in unit_defs}
            df = df.rename(columns=labels)

        # Save to file if output path is provided
        if output:
            df.to_csv(output)

        return df

    except requests.exceptions.RequestException as e:
        print(f"Error connecting to SILO API: {e}")
        print(f"Attempted to access URL: {base_url} with params: {params}")
        return None
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV data: {e}")
        print("API Response:")
        print(text)
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(f"Attempted to access URL: {base_url} with params: {params}")
        return None


# Example using station
data = pointdata(['daily_rain', 'max_temp'], 
                'rebecca.stolper@hotmail.com', 
                'apirequest',
                '20200101', 
                '20201231', 
                station='39083')
                

# Or using lat/lon
data = pointdata(variables=['daily_rain', 'max_temp', 'min_temp'],
                username="EMAIL",
                password="apirequest",
                start="20200101",
                finish="20201231",
                lat=-27.5,
                lon=153.0,
                units=True,
                output="csv")

