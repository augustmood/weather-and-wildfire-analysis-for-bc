import os, sys, re, time, yaml, schedule
from datetime import datetime
from data_fetch import fetch_forecast, fetch_history_update
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

def main(config):

    print(f"update current_weather at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    data_forecast = fetch_forecast()
    data_history = fetch_history_update()

    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect()
    session.execute(f"USE {config['KEYSPACE']}")

    def batch_insert(data, data_type):

        cols = str(tuple(config[f'{data_type.upper()}_COLUMNS'])).replace("'","")
        val_replace = f"({'?, '*(len(config[f'{data_type.upper()}_COLUMNS'])-1)}?)"

        batch = BatchStatement()
        batch_count = 0
        insert_cmd = session.prepare(f"INSERT INTO {data_type}_weather {cols} VALUES {val_replace}")
        insert_cmd.consistency_level = ConsistencyLevel.ONE

        for row in data:
            batch.add(insert_cmd, tuple(row))
            batch_count += 1
            if batch_count == 5:
                session.execute(batch)
                batch.clear()
                batch_count = 0
        
        session.execute(batch)
        batch.clear()
        batch_count = 0

    batch_insert(data_forecast,'forecast')
    batch_insert(data_history,'history')

    session.shutdown()



if __name__=="__main__":

    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    schedule.every().day.at("00:10").do(main, config)
    while True:
        schedule.run_pending()
        time.sleep(60)
