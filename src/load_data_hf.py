import schedule, time, pytz, yaml
from datetime import datetime
from data_provider import DataExtractor

def main(config):
    tz = pytz.timezone(config['TIMEZONE'])
    print(f"Load history & forecast weather data from database at {datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S')}")
    data_extractor = DataExtractor()
    data_extractor.fetch_forecast_weather('./data/forecast_weather.csv')
    data_extractor.fetch_history_weather('./data/history_weather.csv')

if __name__ == "__main__":

    with open('./config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    schedule.every().day.at("00:15").do(main, config)
    while True:
        schedule.run_pending()
        time.sleep(60)