import pandas as pd
import requests, yaml, json, csv, time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import partial

with open('../config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)


locations = [{"q": city+', British Columnbia'} for city in config['CITIES']]
payloads = [{"locations": locations[:int(len(config['CITIES'])/2)]}, 
            {"locations": locations[int(len(config['CITIES'])/2):]}]


def get_last_days(start_date_str=datetime.now().strftime('%Y-%m-%d'), num_days=config['HISTORY_DAYS']):

    date_format = "%Y-%m-%d"
    last_dates = []

    start_date = datetime.strptime(start_date_str, date_format)

    for i in range(num_days):
        last_date = start_date - timedelta(days=i+1)
        last_dates.append(last_date.strftime(date_format))

    return last_dates


def extract_current(input_dict):

    return [
        input_dict['query']['location']['name'],
        input_dict['query']['location']['lat'],
        input_dict['query']['location']['lon'],
        input_dict['query']['current']['last_updated'],
        input_dict['query']['current']['temp_c'],
        input_dict['query']['current']['wind_kph'],
        input_dict['query']['current']['wind_degree'],
        input_dict['query']['current']['wind_dir'],
        input_dict['query']['current']['cloud'],
        input_dict['query']['current']['humidity'],
        input_dict['query']['current']['air_quality']['pm2_5'],
        input_dict['query']['current']['condition']['text'],
        'https:'+input_dict['query']['current']['condition']['icon']
    ]


def extract_history(input_dict):

    result = [
        input_dict['query']['location']['name'],
        input_dict['query']['forecast']['forecastday'][0]['date'],
        input_dict['query']['location']['lat'],
        input_dict['query']['location']['lon'],
        input_dict['query']['forecast']['forecastday'][0]['day']['maxtemp_c'],
        input_dict['query']['forecast']['forecastday'][0]['day']['mintemp_c'],
        input_dict['query']['forecast']['forecastday'][0]['day']['avgtemp_c'],
        input_dict['query']['forecast']['forecastday'][0]['day']['avghumidity'],
        input_dict['query']['forecast']['forecastday'][0]['day']['condition']['text'],
        'https:'+input_dict['query']['forecast']['forecastday'][0]['day']['condition']['icon']
    ]

    for hour in range(24):
        result.append(input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['is_day'])
        result.append(input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['temp_c'])
        result.append(input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['humidity'])
        result.append(input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['wind_mph'])
        result.append(input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['wind_degree'])
        result.append(input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['wind_dir'])
        result.append(input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['cloud'])
        result.append(input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['condition']['text'])
        result.append('https:'+input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['condition']['icon'])

    return result
    


def extract_forecast(input_dict):
    """
    load next 3 days data
    """
    results = []

    for day in range(1, config['FORECAST_DAYS']+1):
        result = [
            input_dict['query']['location']['name'],
            input_dict['query']['forecast']['forecastday'][day]['date'],
            input_dict['query']['location']['lat'],
            input_dict['query']['location']['lon'],
            input_dict['query']['forecast']['forecastday'][day]['day']['maxtemp_c'],
            input_dict['query']['forecast']['forecastday'][day]['day']['mintemp_c'],
            input_dict['query']['forecast']['forecastday'][day]['day']['avgtemp_c'],
            input_dict['query']['forecast']['forecastday'][day]['day']['avghumidity'],
            input_dict['query']['forecast']['forecastday'][day]['day']['condition']['text'],
            'https:'+input_dict['query']['forecast']['forecastday'][day]['day']['condition']['icon'],
            input_dict['query']['forecast']['forecastday'][day]['day']['daily_chance_of_rain'],
            input_dict['query']['forecast']['forecastday'][day]['day']['daily_chance_of_snow']
        ]
        for hour in range(24):
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['is_day'])
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['temp_c'])
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['humidity'])
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['wind_mph'])
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['wind_degree'])
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['wind_dir'])
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['cloud'])
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['condition']['text'])
            result.append('https:'+input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['condition']['icon'])
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['chance_of_rain'])
            result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['chance_of_snow'])
    
        results.append(result)

    return results


def fetch_current(aqi=True):

    """
    Fetch current weather data for all 52 cities in British Columnbia.
    """

    raw_data = []
    
    def fetch_weather(payload):
        retry, attempt = 0, 5
        url = f"http://api.weatherapi.com/v1/current.json?key={config['API_KEY']}&q=bulk{'&aqi=yes' if aqi == True else ''}"
        headers = {'Content-Type': 'application/json'}
        while retry < attempt:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 200:
                raw_data.append(response.json())
                return None
            else:
                print(f"Attempt {retry} failed with status code: {response.status_code}. Retry in 5s")
                retry += 1
                time.sleep(5)
                continue
        print(f"All {attempt} attemps failed. Need human hands on.")

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_weather, payloads)

    full_raw_data = raw_data[0]['bulk'] + raw_data[1]['bulk']

    final_data = [extract_current(d) for d in full_raw_data]
    return final_data


def fetch_forecast(days=config['FORECAST_DAYS']+1, aqi=True):

    """
    Fetch next 3 days forecast weather data for all 52 cities in British Columnbia.
    Granularity: by date & by hour
    """

    raw_data = []

    def fetch_weather(payload):
        retry, attempt = 0, 5
        url = f"http://api.weatherapi.com/v1/forecast.json?key={config['API_KEY']}&q=bulk{'&aqi=yes' if aqi == True else ''}&days={days}"
        headers = {'Content-Type': 'application/json'}
        while retry < attempt:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 200:
                raw_data.append(response.json())
                return None
            else:
                print(f"Attempt {retry} failed with status code: {response.status_code}. Retry in 5s")
                retry += 1
                time.sleep(5)
                continue
        print(f"All {attempt} attemps failed. Need human hands on.")

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_weather, payloads)

    full_raw_data = raw_data[0]['bulk'] + raw_data[1]['bulk']

    final_data = [j for i in [extract_forecast(d) for d in full_raw_data] for j in i]
    return final_data


def fetch_history():

    """
    Fetch last 7 days historical weather data for all 52 cities in British Columnbia.
    Granularity: by date & by hour
    """

    full_raw_data_list = []
    raw_data = []
    history_days = get_last_days()

    def fetch_weather(data_str, payload):
        retry, attempt = 0, 5
        url = f"http://api.weatherapi.com/v1/history.json?key={config['API_KEY']}&q=bulk&dt={data_str}"
        headers = {'Content-Type': 'application/json'}
        while retry < attempt:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 200:
                raw_data.append(response.json())
                return None
            else:
                print(f"Attempt {retry} failed with status code: {response.status_code}. Retry in 5s")
                retry += 1
                time.sleep(5)
                continue
        print(f"All {attempt} attemps failed. Need human hands on.")

    for day in history_days:
        with ThreadPoolExecutor(max_workers=10) as executor:
                partial_fetch_weather = partial(fetch_weather, day)
                executor.map(partial_fetch_weather, payloads)
        full_raw_data_list.append(raw_data[0]['bulk'] + raw_data[1]['bulk'])
        raw_data = []

    final_data = [extract_history(d) for d in [j for i in full_raw_data_list for j in i]]
    return final_data


def fetch_history_update():

    """
    Update last day historical weather data for all 52 cities in British Columnbia.
    Granularity: by date & by hour
    """

    raw_data = []
    
    def fetch_weather(payload):
        retry, attempt = 0, 3
        url = f"http://api.weatherapi.com/v1/history.json?key={config['API_KEY']}&q=bulk&dt={(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')}"
        headers = {'Content-Type': 'application/json'}
        while retry < attempt:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            if response.status_code == 200:
                raw_data.append(response.json())
                return None
            else:
                print(f"Attempt {retry} failed with status code: {response.status_code}. Retry in 5s")
                retry += 1
                time.sleep(5)
                continue
        print(f"All {attempt} attemps failed. Need human hands on.")

    with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(fetch_weather, payloads)

    full_raw_data = raw_data[0]['bulk'] + raw_data[1]['bulk']

    final_data = [extract_history(d) for d in full_raw_data]
    return final_data
    