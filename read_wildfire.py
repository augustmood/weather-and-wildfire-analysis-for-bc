import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, FloatType
import ast

cluster_seeds = ['node1.local', 'node2.local']
spark = SparkSession.builder.appName('test') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
    .getOrCreate()
    # .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.0') \
    # .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions') \
assert spark.version >= '3.0' # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
wildfire = spark.read.format("org.apache.spark.sql.cassandra").options(table="wildfire", keyspace="bla175").load()
wildfire = wildfire.withColumn("longitude", col("coordinates").getItem(0))\
            .withColumn("latitude", col("coordinates").getItem(1)) \
            .drop("coordinates")
wildfire.show(10)
wildfire_pd = wildfire.toPandas()
wildfire_pd["longitude"] = wildfire_pd["longitude"].apply(lambda x: round(float(x), 3))
wildfire_pd["latitude"] = wildfire_pd["latitude"].apply(lambda x: round(float(x), 3))
wildfire_pd["coordinate"] = wildfire_pd.apply(lambda x: [x['longitude'], x['latitude']], axis=1)
wildfire_pd.to_csv('wildfire.csv', index=False)

# def main():
#     wildfire = spark.read.format("org.apache.spark.sql.cassandra").options(table="wildfire", keyspace="bla175").load()
#     # wildfire.createOrReplaceTempView("wildfire")
#     # TODO: round the coordinates, and rename the columns.
#     wildfire_list_df = wildfire.select("fire_num", "load_date", "fire_sz_ha", "fire_stat", "coordinates").toPandas()
#     wildfire_list_df.coordinates = 

# if __name__ == '__main__':
#     cluster_seeds = ['node1.local', 'node2.local']
#     spark = SparkSession.builder.appName('test').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
#     assert spark.version >= '3.0' # make sure we have Spark 3.0+
#     spark.sparkContext.setLogLevel('WARN')
#     sc = spark.sparkContext
#     main()
