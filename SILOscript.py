# get data from lat/long or station
def pointdata_direct_url(variables,
                       username,
                       password=None,
                       start=None,
                       finish=None,
                       station=None,
                       lat=None,
                       lon=None,
                       units=True,
                       output=None):
    """
    Request data from SILO API using direct URL construction instead of params dictionary.
    """
    import requests
    import pandas as pd
    from io import StringIO
    import urllib.parse

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
    
    if start is None or finish is None:
        raise ValueError("'start' and 'finish' dates are required")

    # Determine base URL
    if station is not None:
        base_url = 'https://www.longpaddock.qld.gov.au/cgi-bin/silo/PatchedPointDataset.php'
    else:
        base_url = 'https://www.longpaddock.qld.gov.au/cgi-bin/silo/DataDrillDataset.php'
    
    # Construct URL manually
    url = f"{base_url}?format=csv&dataset=Official&username={urllib.parse.quote(username)}"
    
    if password:
        url += f"&password={urllib.parse.quote(password)}"
    
    url += f"&start={start}&finish={finish}"
    
    if station is not None:
        url += f"&station={station}"
    else:
        url += f"&lat={lat}&lon={lon}"

    # Add a comment parameter like in your sample URL
    url += "&comment=api_request"
    
    print("Requesting URL (sensitive info redacted):", 
          url.replace(username, "USERNAME").replace(password if password else "", "PASSWORD"))
    
    try:
        r = requests.get(url, timeout=30)
        
        # Print response status
        print(f"Response status code: {r.status_code}")
        
        text = r.content.decode()
        
        # Check for error response
        if "Sorry" in text and "missing essential parameters" in text:
            print("API Error Response:")
            print(text)
            return None
        
        print("First 200 characters of response:")
        print(text[:200])
        
        # Try to parse CSV data
        try:
            # First, read the CSV without parsing dates
            df = pd.read_csv(StringIO(text))
            
            # Rename the date column from 'YYYY-MM-DD' to 'date'
            if 'YYYY-MM-DD' in df.columns:
                df = df.rename(columns={'YYYY-MM-DD': 'date'})
            
            # Now parse the date column
            df['date'] = pd.to_datetime(df['date'])
            
            # Set date as index
            df = df.set_index('date')
            
            # Add units to columns names
            if units:
                labels = {}
                for key, val in unit_defs.items():
                    if key in df.columns:
                        labels[key] = '{}_{}'.format(key, val)
                df = df.rename(columns=labels)

            # Write to csv
            if output:
                df.to_csv(output)

            print(f"Successfully parsed data with {len(df)} rows")
            return df
            
        except Exception as parse_err:
            print(f"CSV parsing error: {parse_err}")
            print("First 500 characters of response:")
            print(text[:500])
            return None
        
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to SILO API: {e}")
        print(f"Attempted to access URL: {url}")
        return None
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
        return None

# Call the function
result = pointdata_direct_url(
    variables=['daily_rain', 'max_temp', 'min_temp'],
    username="EMAIL",
    password="apirequest",
    start="20200101",
    finish="20201231",
    lat=-27.5,
    lon=153.0,
    units=True
)

# Display the result
if result is not None:
    print("\nResult DataFrame:")
    print(result.head())  # Display first 5 rows
    print(f"\nTotal rows: {len(result)}")
    print(f"Columns: {result.columns.tolist()}")
else:
    print("\nNo data was returned (result is None)")
