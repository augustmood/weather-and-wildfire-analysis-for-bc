spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions spark_wild_fire_locations.py
