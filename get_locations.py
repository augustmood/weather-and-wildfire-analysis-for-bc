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
    return (longitude, latitude)

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

# Specify the path to your shapefile (without the file extension)
shapefile_path = 'prot_current_fire_polys'

# Read the shapefile into a GeoDataFrame
gdf = gpd.read_file(shapefile_path + '.shp')
# Extract Only FIRE_NUM & geometry
locations = gdf[['FIRE_NUM', 'geometry']]
locations["coordinates"] = locations["geometry"] \
.apply(lambda x: coord_converter(avg_coord(poly_converter(x))))

