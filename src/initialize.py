from cassandra.cluster import Cluster
from datetime import datetime
import os, sys, re, yaml
from cassandra.query import BatchStatement, SimpleStatement
from data_fetch import fetch_history


def main(config):
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
        last_updated TIMESTAMP,
        temp_c DECIMAL,
        wind_kph DECIMAL,
        wind_degree INT,
        wind_dir TEXT,
        cloud INT,
        humidity INT,
        pm2_5 FLOAT,
        condition TEXT,
        condition_icon_link TEXT,
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
        condition_icon_link TEXT,""" + ''.join([f"""
        is_day_at{i} INT, 
        temp_c_at{i} DECIMAL, 
        humidity_at{i} DECIMAL, 
        wind_mph_at{i} DECIMAL, 
        wind_degree_at{i} INT, 
        wind_dir_at{i} TEXT, 
        cloud_at{i} INT, 
        condition_at{i} TEXT, 
        condition_icon_link_at{i} TEXT,""" for i in range(24)]) + """
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
        condition_icon_link TEXT,
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
        condition_icon_link_at{i} TEXT,
        chance_of_rain_at{i} INT,
        chance_of_snow_at{i} INT,""" for i in range(24)]) + """
        PRIMARY KEY (city, date)
    )""")    

    data_history = fetch_history()

    batch_statement = "BEGIN BATCH "
    for row in data_history:
        batch_statement += f"""
        INSERT INTO {config['KEYSPACE']}.history_weather 
        {str(tuple(config['HISTORY_COLUMNS'])).replace("'","")} VALUES {tuple(row)};"""
    batch_statement += "\nAPPLY BATCH;"
    session.execute(batch_statement)

    session.shutdown()



if __name__=="__main__":

    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    main(config)
