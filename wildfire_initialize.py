from cassandra.cluster import Cluster
import yaml, time

def main(config):

    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect()
    session.execute(f"USE {config['KEYSPACE']};")

    session.execute("DROP TABLE IF EXISTS wildfire")

    session.execute("""
       CREATE TABLE IF NOT EXISTS wildfire (
        objectid bigint,
        fire_year bigint,
        fire_num text,
        versn_num bigint,
        fire_sz_ha double,
        source text,
        track_date date,
        load_date date,
        fire_stat text,
        fire_link text,
        feature_cd text,
        geometry text,
        coordinates list<decimal>,
        PRIMARY KEY (fire_num)
        );                 
        """)

    session.shutdown()

if __name__ == '__main__':
    with open('../config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    t1 = time.time()
    main(config)
    t2 = time.time()
    print(f"time cost:{t2-t1}")
