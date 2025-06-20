import cdsapi

client = cdsapi.Client()

dataset = 'reanalysis-era5-pressure-levels'
request = {
  'product_type': ['reanalysis'],
  'variable': ['geopotential'],
  'year': ['2024'],
  'month': ['03'],
  'day': ['01'],
  'time': ['13:00'],
  'pressure_level': ['1000'],
  'data_format': 'grib',
}
target = 'download.grib'

client.retrieve(dataset, request, target)

#dataset = 'reanalysis-era5-single-levels'
#request = {
#    'product_type': ['reanalysis'],
#    'variable': ['2m_temperature'],
#    'year': ['2023', '2024'],
#    'month': ['01', '02'],
#    'day': ['01', '02', '03'],
#    'time': ['06:00', '07:00', '08:00'],
#    'data_format': 'netcdf',
#    'area': [30, 75, 25, 80]
#}

#client = cdsapi.Client()
#client.retrieve(dataset, request).download()

dataset = "reanalysis-era5-single-levels-timeseries"
request = {
    "variable": ["surface_pressure"],
    "location": {"longitude": 120.56919, "latitude": -33.92241},
    "date": ["2023-01-01/2024-03-26"],
    "data_format": "csv"
}

client = cdsapi.Client()
client.retrieve(dataset, request).download()