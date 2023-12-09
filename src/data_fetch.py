import pytz
import requests
import json
import time
import pytz
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import partial

class WeatherDataFetcher:

    def __init__(self, config, retries=5):
        self._config = config
        self._timezone = pytz.timezone(config['TIMEZONE'])
        self._raw_data = []
        self._locations = [{"q": city + ', British Columnbia, Canada'} for city in self._config['CITIES']]
        self._payloads = [{"locations": self._locations[:int(len(self._config['CITIES']) / 2)]},
                         {"locations": self._locations[int(len(self._config['CITIES']) / 2):]}]
        self.retries = retries

    def _get_last_days(self, start_date_str=None, num_days=None):
        if start_date_str is None:
            start_date_str = datetime.now(tz=self._timezone).strftime('%Y-%m-%d')
        num_days = num_days or self._config['HISTORY_DAYS']
        date_format = "%Y-%m-%d"
        last_dates = []
        start_date = datetime.strptime(start_date_str, date_format)

        for i in range(num_days):
            last_date = start_date - timedelta(days=i + 1)
            last_dates.append(last_date.strftime(date_format))

        return last_dates
        
    def _extract_current(self, input_dict):
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
            ('day_' if input_dict['query']['current']['is_day'] == 1 else 'night_') + input_dict['query']['current']['condition']['icon'][-7:-4]
        ]

    def _extract_history(self, input_dict):
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
            'day_' + input_dict['query']['forecast']['forecastday'][0]['day']['condition']['icon'][-7:-4]
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
            result.append(('day_' if input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['is_day'] == 1 else 'night_') + input_dict['query']['forecast']['forecastday'][0]['hour'][hour]['condition']['icon'][-7:-4])

        return result
        
    def _extract_forecast(self, input_dict):
        results = []

        for day in range(1, self._config['FORECAST_DAYS']+1):
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
                'day_' + input_dict['query']['forecast']['forecastday'][day]['day']['condition']['icon'][-7:-4],
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
                result.append(('day_' if input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['is_day'] == 1 else 'night_') + input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['condition']['icon'][-7:-4])
                result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['chance_of_rain'])
                result.append(input_dict['query']['forecast']['forecastday'][day]['hour'][hour]['chance_of_snow'])
        
            results.append(result)

        return results

    def _fetch_weather(self, url, payload):
        retry = 0
        while retry < self.retries:
            response = requests.post(url, headers={'Content-Type': 'application/json'}, data=json.dumps(payload))
            if response.status_code == 200:
                self._raw_data.append(response.json())
                return self._raw_data
            else:
                print(f"Attempt {retry} failed with status code: {response.status_code}. Retry in 5s")
                retry += 1
                time.sleep(5)
                continue
        print(f"All {self.retries} attempts failed. Need human hands on.")
        return None

    def fetch_current(self, aqi=True):

        """
        Fetch current weather data for all cities in British Columnbia city list.
        """
        url = f"http://api.weatherapi.com/v1/current.json?key={self._config['API_KEY']}&q=bulk{'&aqi=yes' if aqi == True else ''}"

        with ThreadPoolExecutor(max_workers=10) as executor:
            partial_fetch_weather = partial(self._fetch_weather, url)
            executor.map(partial_fetch_weather, self._payloads)

        full_raw_data = self._raw_data[0]['bulk'] + self._raw_data[1]['bulk']
        with open('current.json', 'w+') as f:
            json.dump(full_raw_data, f)

        final_data = [self._extract_current(d) for d in full_raw_data]
        self._raw_data.clear()
        return final_data


    def fetch_forecast(self, aqi=True):

        """
        Fetch next 3 days forecast weather data for all cities in British Columnbia city list.
        Granularity: by date & by hour
        """
        url = f"http://api.weatherapi.com/v1/forecast.json?key={self._config['API_KEY']}&q=bulk{'&aqi=yes' if aqi == True else ''}&days={self._config['FORECAST_DAYS']+1}"

        with ThreadPoolExecutor(max_workers=10) as executor:
            partial_fetch_weather = partial(self._fetch_weather, url)
            executor.map(partial_fetch_weather, self._payloads)

        full_raw_data = self._raw_data[0]['bulk'] + self._raw_data[1]['bulk']

        final_data = [j for i in [self._extract_forecast(d) for d in full_raw_data] for j in i]
        self._raw_data.clear()
        return final_data


    def fetch_history(self):

        """
        Fetch last 7 days historical weather data for all cities in British Columnbia city list.
        Granularity: by date & by hour
        """
        full_raw_data_list = []
        history_days = self._get_last_days()

        for day in history_days:
            url = f"http://api.weatherapi.com/v1/history.json?key={self._config['API_KEY']}&q=bulk&dt={day}"
            with ThreadPoolExecutor(max_workers=10) as executor:
                    partial_fetch_weather = partial(self._fetch_weather, url)
                    executor.map(partial_fetch_weather, self._payloads)
            full_raw_data_list.append(self._raw_data[0]['bulk'] + self._raw_data[1]['bulk'])
            self._raw_data.clear()

        final_data = [self._extract_history(d) for d in [j for i in full_raw_data_list for j in i]]
        self._raw_data.clear()
        return final_data


    def fetch_history_update(self):

        """
        Update last day historical weather data for all cities in British Columnbia city list.
        Granularity: by date & by hour
        """
        url = f"http://api.weatherapi.com/v1/history.json?key={self._config['API_KEY']}&q=bulk&dt={(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')}"

        with ThreadPoolExecutor(max_workers=10) as executor:
                partial_fetch_weather = partial(self._fetch_weather, url)
                executor.map(partial_fetch_weather, self._payloads)

        full_raw_data = self._raw_data[0]['bulk'] + self._raw_data[1]['bulk']

        final_data = [self._extract_history(d) for d in full_raw_data]
        self._raw_data.clear()
        return final_data
        