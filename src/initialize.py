import time, yaml
from datetime import datetime
from data_fetch import WeatherDataExtractor
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType

def main(weather_data_fetcher, config):

    print(f"Initalize database at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    data = weather_data_fetcher.fetch_history()

    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect()
    session.execute(f"USE {config['KEYSPACE']}")

    session.execute("DROP TABLE IF EXISTS current_weather")
    session.execute("DROP TABLE IF EXISTS history_weather")
    session.execute("DROP TABLE IF EXISTS forecast_weather")

    session.execute("""
    CREATE TABLE IF NOT EXISTS current_weather (city TEXT,
        lat DECIMAL,
        lon DECIMAL,
        last_updated TEXT,
        temp_c DECIMAL,
        wind_kph DECIMAL,
        wind_degree INT,
        wind_dir TEXT,
        cloud INT,
        humidity INT,
        pm2_5 FLOAT,
        condition TEXT,
        condition_icon_id TEXT,
        PRIMARY KEY (city)
    )""")
    session.execute("""
    CREATE TABLE IF NOT EXISTS history_weather (
        city TEXT,
        date DATE,
        lat DECIMAL,
        lon DECIMAL,
        maxtemp_c DECIMAL,
        mintemp_c DECIMAL,
        avgtemp_c DECIMAL,
        avghumidity DECIMAL,
        condition TEXT,
        condition_icon_id TEXT,""" + ''.join([f"""
        is_day_at{i} INT, 
        temp_c_at{i} DECIMAL, 
        humidity_at{i} DECIMAL, 
        wind_mph_at{i} DECIMAL, 
        wind_degree_at{i} INT, 
        wind_dir_at{i} TEXT, 
        cloud_at{i} INT, 
        condition_at{i} TEXT, 
        condition_icon_id_at{i} TEXT,""" for i in range(24)]) + """
        PRIMARY KEY (city, date)
    )""")
    session.execute("""
    CREATE TABLE IF NOT EXISTS forecast_weather (
        city TEXT,
        date DATE,
        lat DECIMAL,
        lon DECIMAL,
        maxtemp_c DECIMAL,
        mintemp_c DECIMAL,
        avgtemp_c DECIMAL,
        avghumidity DECIMAL,
        condition TEXT,
        condition_icon_id TEXT,
        daily_chance_of_rain INT,
        daily_chance_of_snow INT,
        pm2_5 FLOAT,""" + ''.join([f"""
        is_day_at{i} INT, 
        temp_c_at{i} DECIMAL, 
        humidity_at{i} DECIMAL, 
        wind_mph_at{i} DECIMAL, 
        wind_degree_at{i} INT, 
        wind_dir_at{i} TEXT, 
        cloud_at{i} INT, 
        condition_at{i} TEXT, 
        condition_icon_id_at{i} TEXT,
        chance_of_rain_at{i} INT,
        chance_of_snow_at{i} INT,""" for i in range(24)]) + """
        PRIMARY KEY (city, date)
    )""")    

    cols = str(tuple(config['HISTORY_COLUMNS'])).replace("'","")
    val_replace = f"({'?, '*(len(config['HISTORY_COLUMNS'])-1)}?)"

    batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM, batch_type=BatchType.UNLOGGED)
    batch_count = 0
    insert_history = session.prepare(f"INSERT INTO history_weather {cols} VALUES {val_replace}")

    for row in data:
        batch.add(insert_history, tuple(row))
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

    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    weather_data_fetcher = WeatherDataExtractor(config)

    t1 = time.time()
    main(weather_data_fetcher, config)
    t2 = time.time()
    print(f"time cost:{t2-t1}")
