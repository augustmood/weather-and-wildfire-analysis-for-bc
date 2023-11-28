import requests, yaml, json
from concurrent.futures import ThreadPoolExecutor

with open('config/config.yaml', 'r') as file:
    config = yaml.safe_load(file)


def bulk_weather_request(data_type='current'):

    raw_data = []

    def fetch_weather(payload):
        url = f"http://api.weatherapi.com/v1/{data_type}.json?key={config['API_KEY'][0]}&q=bulk"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            raw_data.append(response.json())
        else:
            print(f"Request failed with status code: {response.status_code}")
            print(response.text)
            return None


    locations = [{"q": city+', British Columnbia'} for city in config['CITIES']]
    payloads = [{
        "locations": locations[:int(len(config['CITIES'])/2)]
    }, 
    {
        "locations": locations[int(len(config['CITIES'])/2):]
    }]

    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(fetch_weather, payloads)

    return raw_data[0]['bulk'] + raw_data[1]['bulk']
    
