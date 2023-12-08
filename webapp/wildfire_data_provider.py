import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import yaml
class WildfireDataExtractor:

    def __init__(self):
        with open('../config/config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        conf = SparkConf()
        # conf.set("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0")
        conf.set("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
        spark = SparkSession.builder \
            .appName("Load Wild Fire Data") \
            .config("spark.cassandra.connection.host", "cassandra.us-west-2.amazonaws.com") \
            .config("spark.cassandra.connection.port", "9142") \
            .config("spark.cassandra.connection.ssl.enabled", "true") \
            .config("spark.cassandra.auth.username", f"{config['AUTH_USERNAME']}") \
            .config("spark.cassandra.auth.password", f"{config['AUTH_PASSWORD']}") \
            .config(conf = conf)\
            .getOrCreate()
        assert spark.version >= '3.0' # make sure we have Spark 3.0+
        spark.sparkContext.setLogLevel('WARN')
        self._spark = spark

    def fetch_wildfire(self):
        wildfire = self._spark.read.format("org.apache.spark.sql.cassandra").options(table="wildfire", keyspace="bla175").load()
        wildfire = wildfire.withColumn("longitude", col("coordinates").getItem(0))\
                    .withColumn("latitude", col("coordinates").getItem(1)) \
                    .drop("coordinates")
        wildfire.show(10)
        wildfire_pd = wildfire.toPandas()
        wildfire_pd["longitude"] = wildfire_pd["longitude"].apply(lambda x: round(float(x), 3))
        wildfire_pd["latitude"] = wildfire_pd["latitude"].apply(lambda x: round(float(x), 3))
        wildfire_pd["coordinate"] = wildfire_pd.apply(lambda x: [x['longitude'], x['latitude']], axis=1)
        return wildfire_pd
