from shapely.geometry import Point
import geopandas as gpd


def addPopDen(fireDF):
    print("loading population grid:")
    popData = gpd.read_file("Shape\\ESD2Pop.shp")
    # specify that source data is WGS 84 / Pseudo-Mercator -- Spherical Mercator, Google Maps, OpenStreetMap, Bing, ArcGIS, ESRI' - https://epsg.io/3857
    popData.set_crs(epsg=3857, inplace=True)
    # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    popData = popData.to_crs(4326)

    # weird aliases... this is total population / AreaofLAND(meters)
    popData["Pop/Mile"] = popData.apply(
        lambda x: (x["B01001_001"] / x["ALAND"]) * 2590000, axis=1
    )

    def getPopulationDensity(lon, lat):
        plot = Point(lon, lat)
        try:
            # return None
            mapInd = (popData.index[popData.contains(plot)])[0]
            # population = popData.loc[mapInd, "B01001_001E"]
            # area = popData.loc[mapInd, "Shape_Area"]
            return popData.loc[mapInd, "Pop/Mile"]
            # return mapInd
        except:
            return None

    from tqdm import tqdm

    tqdm.pandas(
        desc=f"finding Population Data:",
        leave=False,
    )
    fireDF["People/Mile"] = fireDF.progress_apply(
        lambda x: getPopulationDensity(x["X-Long"], x["Y_Lat"]),
        axis=1,
    )

    def popRatio(pop):
        if pop < 1000:
            return "rural"
        if pop < 2000:
            return "suburban"
        return "urban"

    fireDF["Population Classification"] = fireDF.apply(
        lambda x: popRatio(x["People/Mile"]), axis=1
    )

    return fireDF
