import time, yaml, pytz
from datetime import datetime
from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider

def main(config):
    tz = pytz.timezone(config['TIMEZONE'])
    print(f"Initialize wildfire data at {datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S')}")
    ssl_context = SSLContext(PROTOCOL_TLSv1_2 )
    ssl_context.load_verify_locations('./config/sf-class2-root.crt')
    ssl_context.verify_mode = CERT_REQUIRED
    auth_provider = PlainTextAuthProvider(username=f"{config['AUTH_USERNAME']}", password=f"{config['AUTH_PASSWORD']}")
    cluster = Cluster(['cassandra.us-west-2.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
    # session_drop = cluster.connect()
    # session_drop.execute(f"USE {config['KEYSPACE']}")
    # session_drop.execute("DROP TABLE IF EXISTS wildfire")
    # session_drop.shutdown()
    session_create = cluster.connect()
    session_create.execute(f"USE {config['KEYSPACE']}")
    session_create.execute("""
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

    session_create.shutdown()

if __name__ == '__main__':

    with open('./config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    t1 = time.time()
    main(config)
    t2 = time.time()
    print(f"time cost:{t2-t1}")
