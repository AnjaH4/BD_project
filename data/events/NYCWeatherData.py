import requests
from datetime import date
import pandas as pd
import io
import json

def get_noaa_data(url, params, header):

    r = requests.get(url, params, headers=header).text
    rawData = pd.read_csv(io.StringIO(r))
    return rawData

def get_forecast_data(url):
    r = requests.get(url)
    json1_data = json.loads(r.text)
    df = pd.DataFrame(json1_data['properties']['periods'])
    print(r.text)
    return df

# use if you want borough specific data
stations_dict_borough = {
    'MANHATTAN': ['USW00094728'],
    'BRONX': ['USW00094789'], #?? i dont think so
    'BROOKLYN': ['US1NYKN0025'],
    'QUEENS': ['USW00094789'],
    }

if __name__ == '__main__':

    today = date.today()
    
    # NCEI Data Service API
    for year in range(2014, 2025):
        token = 'pDItnczjGvMQxqDxjwzlywJbgmtsHqNZ'
        creds = dict(token=token)
        prev_year = year - 1

        # Data for Central Park, Manhattan
        # Fiscal year: July 1, year to June 30, year
        # Data types: Average wind speed, Precipitation, Snowfall, Average temperature, Minimum temperature, Maximum temperature
        
        params = {'dataset': 'daily-summaries', 'stations': 'USW00094728','startDate': f'{prev_year}-07-01',\
            'endDate': f'{year}-06-30',  'dataTypes': ['AWND', 'PRCP', 'SNOW', 'TAVG', 'TMIN', 'TMAX'], 'units': 'standard'}
        url = 'https://www.ncei.noaa.gov/access/services/data/v1'
        print("Getting data from NOAA API")
        urlData = get_noaa_data(url, params, creds)
        print(urlData.head())
        urlData.to_csv(f'data\weather\weather_{year}.csv', index=False)
        print(f"Data saved to weather_{year}.csv")



