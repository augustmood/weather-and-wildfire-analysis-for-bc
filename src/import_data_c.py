from cassandra.cluster import Cluster
from datetime import datetime
import os, sys, re, time, yaml, schedule
from data_fetch import fetch_current
from cassandra.query import BatchStatement, SimpleStatement

def main(config):

    data_current = fetch_current()

    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect()
    session.execute(f"USE {config['KEYSPACE']}")
    session.execute("TRUNCATE TABLE current_weather")

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

    schedule.every().hour.at(":00").do(main(config))
    while True:
        schedule.run_pending()
        time.sleep(60)