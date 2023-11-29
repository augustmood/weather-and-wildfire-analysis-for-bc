import requests, yaml, json
import aiohttp, asyncio
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import partial

with open('config/config.yaml', 'r') as file:
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

    return {
        'name': input_dict['query']['location']['name'],
        'lat': input_dict['query']['location']['lat'],
        'lon': input_dict['query']['location']['lon'],
        'last_updated': input_dict['query']['current']['last_updated'],
        'temp_c': input_dict['query']['current']['temp_c'],
        'wind_kph': input_dict['query']['current']['wind_kph'],
        'wind_degree': input_dict['query']['current']['wind_degree'],
        'wind_dir': input_dict['query']['current']['wind_dir'],
        'cloud': input_dict['query']['current']['cloud'],
        'humidity': input_dict['query']['current']['humidity'],
        'pm2_5': input_dict['query']['current']['air_quality']['pm2_5'],
        'condition': input_dict['query']['current']['condition']['text'],
        'condition_icon_link': 'https://'+input_dict['query']['current']['condition']['icon']
}


def extract_history(input_dict):

    result = {
        'name': input_dict['query']['location']['name'],
        'lat': input_dict['query']['location']['lat'],
        'lon': input_dict['query']['location']['lon'],
        'date': input_dict['query']['forecast']['forecastday'][0]['date'],
        'maxtemp_c': input_dict['query']['forecast']['forecastday'][0]['day']['maxtemp_c'],
        'mintemp_c': input_dict['query']['forecast']['forecastday'][0]['day']['mintemp_c'],
        'avgtemp_c': input_dict['query']['forecast']['forecastday'][0]['day']['avgtemp_c'],
        'avghumidity': input_dict['query']['forecast']['forecastday'][0]['day']['avghumidity'],
        'condition': input_dict['query']['forecast']['forecastday'][0]['day']['condition']['text'],
        'condition_icon_link': 'https:'+input_dict['query']['forecast']['forecastday'][0]['day']['condition']['icon']
    }

    for hour in range(24):
        result[f'is_day_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['is_day']
        result[f'temp_c_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['temp_c']
        result[f'humidity_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['humidity']
        result[f'wind_mph_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['wind_mph']
        result[f'wind_degree_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['wind_degree']
        result[f'wind_dir_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['wind_dir']
        result[f'cloud_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['cloud']
        result[f'condition_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['condition']['text']
        result[f'condition_icon_link_at{hour}'] = 'https:'+input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['condition']['icon']
        result[f'chance_of_rain_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['chance_of_rain']
        result[f'chance_of_snow_at{hour}'] = input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['chance_of_snow']
    
    return result
    


def extract_forecast(input_dict):
    """
    load next 3 days data
    """
    results = []

    for day in range(1, config['FORECAST_DAYS']+1):
        result = {
            'name': input_dict['query']['location']['name'],
            'lat': input_dict['query']['location']['lat'],
            'lon': input_dict['query']['location']['lon'],
            'date': input_dict['query']['forecast']['forecastday'][day]['date'],
            'maxtemp_c': input_dict['query']['forecast']['forecastday'][day]['day']['maxtemp_c'],
            'mintemp_c': input_dict['query']['forecast']['forecastday'][day]['day']['mintemp_c'],
            'avgtemp_c': input_dict['query']['forecast']['forecastday'][day]['day']['avgtemp_c'],
            'avghumidity': input_dict['query']['forecast']['forecastday'][day]['day']['avghumidity'],
            'daily_chance_of_rain': input_dict['query']['forecast']['forecastday'][day]['day']['daily_chance_of_rain'],
            'daily_chance_of_snow': input_dict['query']['forecast']['forecastday'][day]['day']['daily_chance_of_snow'],
            'pm2_5': input_dict['query']['forecast']['forecastday'][day]['day']['air_quality']['pm2_5'],
            'condition': input_dict['query']['forecast']['forecastday'][day]['day']['condition']['text'],
            'condition_icon_link': 'https:'+input_dict['query']['forecast']['forecastday'][day]['day']['condition']['icon']
        }
        for hour in range(24):
            result[f'is_day_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['is_day']
            result[f'temp_c_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['temp_c']
            result[f'humidity_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['humidity']
            result[f'wind_mph_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['wind_mph']
            result[f'wind_degree_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['wind_degree']
            result[f'wind_dir_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['wind_dir']
            result[f'cloud_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['cloud']
            result[f'condition_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['condition']['text']
            result[f'condition_icon_link_at{hour}'] = 'https:'+input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['condition']['icon']
            result[f'chance_of_rain_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['chance_of_rain']
            result[f'chance_of_snow_at{hour}'] = input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['chance_of_snow']
    
        results.append(result)

    return results


def fetch_current(aqi=True):

    """
    Fetch current weather data for all 52 cities in British Columnbia.
    """

    raw_data = []

    def fetch_weather(payload):
        url = f"http://api.weatherapi.com/v1/current.json?key={config['API_KEY'][0]}&q=bulk{'&aqi=yes' if aqi == True else ''}"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            raw_data.append(response.json())
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(response.text)
            return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_weather, payloads)

    full_raw_data = raw_data[0]['bulk'] + raw_data[1]['bulk']

    return [extract_current(d) for d in full_raw_data]


def fetch_forecast(days=config['FORECAST_DAYS']+1, aqi=True):

    """
    Fetch next 3 days forecast weather data for all 52 cities in British Columnbia.
    Granularity: by date
    """

    raw_data = []

    def fetch_weather(payload):
        url = f"http://api.weatherapi.com/v1/forecast.json?key={config['API_KEY']}&q=bulk{'&aqi=yes' if aqi == True else ''}&days={days}"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            raw_data.append(response.json())
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(response.text)
            return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_weather, payloads)

    full_raw_data = raw_data[0]['bulk'] + raw_data[1]['bulk']

    return [j for i in [extract_forecast(d) for d in full_raw_data] for j in i]
    # return full_raw_data


def fetch_history():

    """
    Fetch last 7 days historical weather data for all 52 cities in British Columnbia.
    Granularity: by date
    """

    full_raw_data_list = []
    raw_data = []
    history_days = get_last_days()

    def fetch_weather(date_str, payload):
        url = f"http://api.weatherapi.com/v1/history.json?key={config['API_KEY']}&q=bulk&dt={date_str}"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            raw_data.append(response.json())
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(response.text)
            return None

    for day in history_days:
        with ThreadPoolExecutor(max_workers=10) as executor:
                partial_fetch_weather = partial(fetch_weather, day)
                executor.map(partial_fetch_weather, payloads)
        full_raw_data_list.append(raw_data[0]['bulk'] + raw_data[1]['bulk'])
        raw_data = []

    return [extract_history(d) for d in [j for i in full_raw_data_list for j in i]]

    