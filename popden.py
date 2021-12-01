from shapely.geometry import Point
import pandas as pd
import geopandas as gpd


def addPopDen(fireDF):
    print("loading population grid:")
    popData = gpd.read_file("Shape\\ESD2Pop.shp")
    # specify that source data is WGS 84 / Pseudo-Mercator -- Spherical Mercator, Google Maps, OpenStreetMap, Bing, ArcGIS, ESRI' - https://epsg.io/3857
    popData.set_crs(epsg=3857, inplace=True)
    # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    popData = popData.to_crs(4326)

    # weird aliases... this is 'total population' / 'AreaofLAND(meters)'
    popData["Pop/Mile"] = popData.apply(
        lambda x: (x["B01001_001"] / x["ALAND"]) * 2590000, axis=1
    )

    def getPopulationDensity(lon, lat):
        plot = Point(lon, lat)
        try:
            mapInd = (popData.index[popData.contains(plot)])[0]
            return popData.loc[mapInd, "Pop/Mile"]
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
        if pd.isnull(pop):
            return "Outside ESD2"
        if pop < 1000:
            return "Rural"
        if pop < 2000:
            return "Suburban"
        return "Urban"

    fireDF["Population Classification"] = fireDF.apply(
        lambda x: popRatio(x["People/Mile"]), axis=1
    )

    return fireDF
