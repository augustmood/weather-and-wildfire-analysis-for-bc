import time, yaml, schedule, pytz
from datetime import datetime
from data_fetch import WeatherDataFetcher
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, BatchType
from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider

def main(weather_data_fetcher, config):

    tz = pytz.timezone(config['TIMEZONE'])
    print(f"update current_weather at {datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S')}")
    data_current = weather_data_fetcher.fetch_current()

    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.load_verify_locations('./config/sf-class2-root.crt')
    ssl_context.verify_mode = CERT_REQUIRED
    auth_provider = PlainTextAuthProvider(username=config['USERNAME'], password=config['PASSWORD'])
    cluster = Cluster(['cassandra.us-west-2.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
    session = cluster.connect()
    session.execute(f"USE {config['KEYSPACE']}")

    
    cols = str(tuple(config[f'CURRENT_COLUMNS'])).replace("'","")
    val_replace = f"({'?, '*(len(config[f'CURRENT_COLUMNS'])-1)}?)"

    batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM, batch_type=BatchType.UNLOGGED)
    batch_count = 0
    insert_cmd = session.prepare(f"INSERT INTO current_weather {cols} VALUES {val_replace}")

    for row in data_current:
        batch.add(insert_cmd, tuple(row))
        batch_count += 1
        if batch_count == 5:
            session.execute(batch)
            batch.clear()
            batch_count = 0

    session.execute(batch)
    batch.clear()
    batch_count = 0

    session.shutdown()

if __name__=="__main__":

    with open('./config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    weather_data_fetcher = WeatherDataFetcher(config)
    schedule.every().hour.at(":03").do(main, weather_data_fetcher, config)
    while True:
        schedule.run_pending()
        time.sleep(60)