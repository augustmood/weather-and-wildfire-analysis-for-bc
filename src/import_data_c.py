from cassandra.cluster import Cluster
from datetime import datetime
import os, sys, re, yaml
from cassandra.query import BatchStatement, SimpleStatement
from data_fetch import fetch_current


def main(config):
    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect()
    session.execute(f"USE {config['KEYSPACE']}")
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

    data_current = fetch_current()

    batch_statement = "BEGIN BATCH "
    for row in data_current:
        batch_statement += f"""
        INSERT INTO {config['KEYSPACE']}.current_weather 
        {str(tuple(config['CURRENT_COLUMNS'])).replace("'","")} VALUES {tuple(row)};"""
    batch_statement += "\nAPPLY BATCH;"
    session.execute(batch_statement)

    session.shutdown()



if __name__=="__main__":

    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    main(config)
