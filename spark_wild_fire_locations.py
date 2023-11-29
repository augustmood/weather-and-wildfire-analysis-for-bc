import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
import geopandas as gpd
from pyproj import Proj, transform
from shapely.geometry import Polygon, MultiPolygon
import numpy as np

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

def main(inputs, keyspace, table):
    # Specify the path to your shapefile (without the file extension)
    shapefile_path = inputs
    # Read the shapefile into a GeoDataFrame
    gdf = gpd.read_file(shapefile_path)
    # gdf = gpd.read_file(shapefile_path + '.shp')
    # Extract Only FIRE_NUM & geometry
    locations = gdf[['FIRE_NUM', 'geometry']]
    # locations["coordinates"] = locations["geometry"] \
    # .apply(lambda x: coord_converter(avg_coord(poly_converter(x))))
    locations["geometry"] = locations["geometry"] \
    .apply(lambda x: coord_converter(avg_coord(poly_converter(x))))
    locations_df = spark.createDataFrame(locations)
    locations_df = locations_df.withColumnRenamed("FIRE_NUM", "fire_num")
    locations_df.show(20)
    locations_df.write.format("org.apache.spark.sql.cassandra").mode("overwrite").option("confirm.truncate", "true").options(table=table, keyspace=keyspace).save()

if __name__ == '__main__':
    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Load Logs Spark').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, keyspace, table)
