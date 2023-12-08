import schedule, time
from data_provider import DataExtractor

def main():
    data_extractor = DataExtractor()
    data_extractor.fetch_forecast_weather('../data/forecast_weather.csv')
    data_extractor.fetch_history_weather('../data/history_weather.csv')

if __name__ == "__main__":
    schedule.every().day.at("00:15").do(main)
    while True:
        schedule.run_pending()
        time.sleep(60)