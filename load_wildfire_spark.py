import sys
assert sys.version_info >= (3, 5)  # Ensure Python version is 3.5 or above
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import udf
from simpledbf import Dbf5
import zipfile
import uuid

def extract_zip(zip_file_path, extract_path):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

def create_spark_session(cluster_seeds):
    return SparkSession.builder \
        .appName('Spark Cassandra') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)) \
        .getOrCreate()

def generate_uuid():
    return str(uuid.uuid4())

def main(): 
    zip_file_path = 'prot_current_fire_polys.zip'
    extract_path = 'extracted_data'

    extract_zip(zip_file_path, extract_path)

    dbf = Dbf5('prot_current_fire_polys.dbf') 
    pandas_df = dbf.to_dataframe()
    pandas_df.columns = map(str.lower, pandas_df.columns)

    spark = create_spark_session(['node1.local', 'node2.local'])
    spark_df = spark.createDataFrame(pandas_df)

    uuid_udf = udf(generate_uuid, types.StringType())
    df_with_uuid = spark_df.withColumn("id", uuid_udf())

    # Uncomment the lines below if you want to see the DataFrame information
    df_with_uuid.show(10000)
    df_with_uuid.printSchema()

    df_with_uuid.write.format("org.apache.spark.sql.cassandra") \
        .options(table='wildfiretable', keyspace='dya63') \
        .mode("overwrite").option("confirm.truncate","true").save()

if __name__ == '__main__':
    main()