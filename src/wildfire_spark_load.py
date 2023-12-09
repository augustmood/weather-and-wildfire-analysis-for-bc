import sys
import numpy as np
import pandas as pd
import yaml
import geopandas as gpd
import zipfile
import schedule, time
import requests
import os
import pytz
from datetime import datetime
from pyspark.sql import SparkSession
from pyproj import Proj, transform
from shapely.geometry import Polygon, MultiPolygon
from shapely.wkt import loads
from simpledbf import Dbf5
from pyspark import SparkConf
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# function to transfer EPSG: 3005 Geographic coordinates to EPSG 4326 Coordinates.

def coord_converter(inputs):
    source_crs = Proj(init='epsg:3005')  # NAD83 / BC Albers
    target_crs = Proj(init='epsg:4326')  # WGS 84
    east, north = inputs
    longitude, latitude = transform(source_crs, target_crs, east, north)
    return [longitude, latitude]

# function to transfer polygon to 2-D arrays.

def poly_converter(inputs):
    if (isinstance(inputs, Polygon)):
        return np.array(inputs.exterior.coords)
    elif (isinstance(inputs, MultiPolygon)):
        inputs = inputs.geoms
        arr = []
        for polygon in inputs:
            poly_array = np.array(polygon.exterior.coords)
            arr.extend(poly_array.tolist())
        return np.array(arr)
    else:
        TypeError("Not a PolyGon or MultiPolygon")

# take the average of the list of coordinates as the location identification 
# coordinate.
def avg_coord(inputs):
    avg_coord = np.mean(inputs, axis=0)
    return avg_coord

# Converting Polygon/MultiPolygon strings to Polygon/MultiPolygon objects
def load_polygon(poly_str):
    loads(poly_str)

# extract file from the auto-downloaded zip file:
# Overwrites the existing files by default.
def extract_zip(zip_file_path, extract_path):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

# Create table on Cassandra:
# create table wildfire(fire_num text PRIMARY KEY, coordinates list<decimal>, geometry text);

def download_file(url, local_filename):
    with requests.get(url, stream=True) as response:
        if response.status_code == 200:
            with open(local_filename, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded: {local_filename}")
        else:
            print(f"Failed to download: {url}")
            print(f"Status code: {response.status_code}")

def main(config):
    tz = pytz.timezone(config['TIMEZONE'])
    print(f"Initialize database at {datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S')}")
    conf = SparkConf()
    conf.set("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
    spark = SparkSession.builder \
        .appName("Load Wild Fire Data") \
        .config("spark.cassandra.connection.host", "cassandra.us-west-2.amazonaws.com") \
        .config("spark.cassandra.connection.port", "9142") \
        .config("spark.cassandra.connection.ssl.enabled", "true") \
        .config("spark.cassandra.auth.username", f"{config['AUTH_USERNAME']}") \
        .config("spark.cassandra.auth.password", f"{config['AUTH_PASSWORD']}") \
        .config(conf=conf)\
        .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    file_url = "https://pub.data.gov.bc.ca/datasets/cdfc2d7b-c046-4bf0-90ac-4897232619e1/prot_current_fire_polys.zip"
    dir_path = "./data/"
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    file_path = "./data/prot_current_fire_polys.zip"
    if os.path.exists("./data/prot_current_fire_polys.zip"):
        os.remove("./data/prot_current_fire_polys.zip")
    download_file(file_url, file_path)

    pd.DataFrame.iteritems = pd.DataFrame.items
    # Specify the path to your shapefile (without the file extension)
    zip_file_path = './data/prot_current_fire_polys.zip'
    extract_path = './data/prot_current_fire_polys'
    extract_zip(zip_file_path, extract_path)
    shapefile_path = './data/prot_current_fire_polys/prot_current_fire_polys.shp'
    # Read the shapefile into a GeoDataFrame
    gdf = gpd.read_file(shapefile_path)
    locations = gdf[['FIRE_NUM', 'geometry']]
    locations["coordinates"] = locations["geometry"] \
    .apply(lambda x: coord_converter(avg_coord(poly_converter(x))))
    locations = locations.drop('geometry', axis=1)
    locations_df = spark.createDataFrame(locations).repartition(100)
    locations_df = locations_df.withColumnRenamed("FIRE_NUM", "fire_num")

    dbf = Dbf5('./data/prot_current_fire_polys/prot_current_fire_polys.dbf') 
    wildfire = dbf.to_dataframe()
    wildfire.columns = map(str.lower, wildfire.columns)
    wildfire["fire_stat"] = wildfire["fire_stat"].apply(lambda x: str(x))
    wildfire["fire_link"] = wildfire["fire_link"].apply(lambda x: str(x))
    wildfire_df = spark.createDataFrame(wildfire).repartition(100)


    wildfire_df = wildfire_df.withColumnRenamed("FIRE_NUM", "fire_num")
    join_cond = [locations_df.fire_num == wildfire_df.fire_num]
    wildfire_table = wildfire_df.join(locations_df, join_cond)\
        .drop(locations_df.fire_num)
    
    wildfire_table = wildfire_table
    wildfire_table.write.format("org.apache.spark.sql.cassandra")\
        .options(table='wildfire', keyspace='bla175')\
    .mode("append").save()

if __name__ == '__main__':

    with open('./config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    schedule.every().hour.at(":03").do(main, config)
    while True:
        schedule.run_pending()
        time.sleep(60)
