from shapely.geometry import Point
import pandas as pd
import geopandas as gpd


def addPopDen(fireDF):
    print("loading population grid:")
    # popData = gpd.read_file("Shape\\ESD2Pop.shp")
    popData = gpd.read_file("Shape\\PopulationDensityInESD2.shp")
    # ESD2Pop needed this, PopDenInESD2 is already in 4326
    # specify that source data is WGS 84 / Pseudo-Mercator -- Spherical Mercator, Google Maps, OpenStreetMap, Bing, ArcGIS, ESRI' - https://epsg.io/3857
    # popData.set_crs(epsg=3857, inplace=True)
    # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    # popData = popData.to_crs(4326)

    # weird aliases... this is 'total population' / 'AreaofLAND(meters)'
    # population_name = "B01001_001"
    # area_name = "ALAND"

    # popData["Pop/Mile"] = popData.apply(
    #     lambda x: (x[population_name] / x[area_name]) * 2590000, axis=1
    # )
    popData["Pop/Mile"] = popData.apply(lambda x: x["POP_SQMI"], axis=1)

    # Also load in Block Data from
    print("loading Block Data:")
    blockData = gpd.read_file("Shape\\BlockData.shp")
    blockData.set_crs(epsg=4326, inplace=True)

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

    def getBlockData(lon, lat):
        plot = Point(lon, lat)
        try:
            # print(blockData)
            mapInd = blockData.index[blockData.contains(plot)][0]
            # GEOID20 = FIPS Alias
            return blockData.loc[mapInd, "GEOID20"]
            # return mapInd
        except:
            return None

    fireDF["blockData"] = fireDF.apply(
        lambda x: getBlockData(x["X-Long"], x["Y_Lat"]),
        axis=1,
    )

    return fireDF


def main():
    from pandasgui import show

    file = "Fire.xlsx"
    df = pd.read_excel(file)
    df = addPopDen(df)

    def getQuery(block, inc):
        return f"UPDATE [dbo].[FireIncidents] set [Block_ID] = '{block}' WHERE [Incident_Number] = '{inc}'"

    df["sql_statement"] = df.apply(
        lambda x: getQuery(x["blockData"], x["Incident_Number"]),
        axis=1,
    )


if __name__ == "__main__":
    main()
