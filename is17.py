# from shapely.geometry import MultiPolygon
from shapely.geometry import Point, Polygon, MultiPolygon
import geopandas as gpd

esd17 = gpd.read_file("esd17.shp")

# specify that source data is 'NAD 1983 StatePlane Texas Central FIPS 4203 (US Feet)' - https://epsg.io/2277
esd17.set_crs(epsg=2277, inplace=True)
# and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
esd17 = esd17.to_crs(4326)

p1 = Point(-97.60254748509955, 30.398634794025643)
print(
    (esd17.contains(p1)).any()
)  # check if this point is in any of the polygons included in the shape file
