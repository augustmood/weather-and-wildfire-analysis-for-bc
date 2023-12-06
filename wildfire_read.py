import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, FloatType
import ast

def main():
    wildfire = spark.read.format("org.apache.spark.sql.cassandra").options(table="wildfire", keyspace="bla175").load()
    wildfire = wildfire.withColumn("longitude", col("coordinates").getItem(0))\
                .withColumn("latitude", col("coordinates").getItem(1)) \
                .drop("coordinates")
    wildfire.show(10)
    wildfire_pd = wildfire.toPandas()
    wildfire_pd["longitude"] = wildfire_pd["longitude"].apply(lambda x: round(float(x), 3))
    wildfire_pd["latitude"] = wildfire_pd["latitude"].apply(lambda x: round(float(x), 3))
    wildfire_pd["coordinate"] = wildfire_pd.apply(lambda x: [x['longitude'], x['latitude']], axis=1)
    # wildfire_pd.to_csv('wildfire.csv', index=False)

if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("Load Wild Fire Data") \
        .config("spark.cassandra.connection.host", "cassandra.us-west-2.amazonaws.com") \
        .config("spark.cassandra.connection.port", "9142") \
        .config("spark.cassandra.connection.ssl.enabled", "true") \
        .config("spark.cassandra.auth.username", "bin-ming-at-872464001298") \
        .config("spark.cassandra.auth.password", "O7k1jKqgvzG+Fbw2EsM7HGN8Pc0tEMYMWqr/cgrj3kI=") \
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
