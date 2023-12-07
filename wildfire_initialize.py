from cassandra.cluster import Cluster
import yaml, time
import time, yaml
from cassandra.cluster import Cluster
from cassandra.cluster import Cluster
from ssl import SSLContext, PROTOCOL_TLSv1_2 , CERT_REQUIRED
from cassandra.auth import PlainTextAuthProvider

def main():
    ssl_context = SSLContext(PROTOCOL_TLSv1_2 )
    ssl_context.load_verify_locations('config/sf-class2-root.crt')
    ssl_context.verify_mode = CERT_REQUIRED
    auth_provider = PlainTextAuthProvider(username='bin-ming-at-872464001298', password='O7k1jKqgvzG+Fbw2EsM7HGN8Pc0tEMYMWqr/cgrj3kI=')
    cluster = Cluster(['cassandra.us-west-2.amazonaws.com'], ssl_context=ssl_context, auth_provider=auth_provider, port=9142)
    session = cluster.connect()
    session.execute("USE bla175")
    # session.execute("DROP TABLE IF EXISTS wildfire")

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
    t1 = time.time()
    main()
    t2 = time.time()
    print(f"time cost:{t2-t1}")
