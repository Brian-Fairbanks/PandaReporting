from geopandas import geodataframe
import pyproj
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import transform
import osmnx as ox
from osmnx import distance as dist
import networkx as nx
import matplotlib.pyplot as plt
from os.path import exists
import traceback
import numpy as np
from geopandas import GeoDataFrame
from timer import Timer

from tqdm import tqdm


# This really is acting more like a class than a set of functions, but I really need to look into proper class declaration for python 3 ...

station = ""
roadMap = ""
distBuf = 10000  # 10 for testing, so everything goes much faster.  Actual data should be 10000 (~6.2 miles)


def toCrs(lat, lon):
    wgs84 = pyproj.CRS("EPSG:4326")
    local = pyproj.CRS("EPSG:2277")

    wgs84_pt = Point(lat, lon)

    project = pyproj.Transformer.from_crs(wgs84, local, always_xy=True).transform
    return transform(project, wgs84_pt)


def setStation(coords):
    """
    sets a current station on the map

    Parameters
    --------------------------------
    Lat : float
        latitude of location
    Lon : float
        longituted of location

    Returns
    --------------------------------
    Nearest node id to location
    """
    global roadMap
    global station

    point = toCrs(coords[1], coords[0])

    node = ox.nearest_nodes(roadMap, point.x, point.y)
    nodeDist = ox.nearest_nodes(roadMap, point.x, point.y, return_dist=True)

    # print("===== Station node:distance - \n", nodeDist)
    # print(point)
    # print(roadMap.nodes[node], "\n===============================\n")

    station = node
    return station


def distToStationFromGPS(lat, lon):
    """
    returns the distance from a passed lat lon set to a station -
    requires a station be set.  May crash if no station is set.

    Parameters
    --------------------------------
    Lat : float
        latitude of location
    Lon : float
        longituted of location

    Returns
    --------------------------------
    Float
        shortest distance along roadways between passed location and station
    """
    global station

    # get the nearest network node to each point
    point = toCrs(lon, lat)

    dest_node = ox.nearest_nodes(roadMap, point.x, point.y)

    nodeDist = ox.nearest_nodes(roadMap, point.x, point.y, return_dist=True)

    # if the incident is more than .3 miles from nearest node, something is problematic
    # if nodeDist[1] > 500:
    #     return -1

    # print("===== Station node:distance - \n", nodeDist)
    # print(point)
    # print(roadMap.nodes[dest_node], "\n===============================\n")

    # dest_node = ox.nearest_nodes(roadMap, lon, lat)

    # print(station, ":", dest_node)

    try:
        # how long is our route in meters?
        # dist = ox.shortest_path(roadMap, station, dest_node, weight="length")
        dist = nx.shortest_path_length(roadMap, station, dest_node, weight="length")
        # print("shortest path is:", dist)
    except:
        if station == "":
            print(
                "You have likely just attempted to find the distance from a station, without first setting a station (setStation(lat,lon))"
            )
        else:
            print(
                "error getting distance between: ", station, " & ", dest_node
            )  ## usually the map is to small and a connection cannot be found
            traceback.print_stack()
        return None

    distInMiles = dist * float(0.000621371)
    return distInMiles


def distToStationFromNode(dest_node, fullProgress=None):
    """
    returns the distance from a passed map node -
    requires a station be set.  May crash if no station is set.

    Parameters
    --------------------------------
    dest_node : str
        strin index of a node on roadMap
    (Optional) : tqdm progress bar


    Returns
    --------------------------------
    Float
        shortest distance along roadways between passed location and station
    """
    if fullProgress is not None:
        fullProgress.update(1)

    if pd.isnull(dest_node):
        return None
    try:
        # how long is our route in meters?
        dist = nx.shortest_path_length(roadMap, station, dest_node, weight="length")
    except:
        if station == "":
            print(
                "You have likely just attempted to find the distance from a station, without first setting a station (setStation(lat,lon))"
            )
        # else:
        # print("error getting finding path between: ", station, " & ", dest_node)
        ## usually the map is to small and a connection cannot be found
        # traceback.print_stack()
        return None

    distInMiles = dist * float(0.000621371)
    return distInMiles


# ##############################################################################################################################################
#     GDF Addition Functions
# ##############################################################################################################################################


def getPoint(point, type):
    if type not in ["ENG", "QNT"]:
        return None
    return ox.nearest_nodes(roadMap, point.x, point.y)


def addNearestNodeToGDF(gdf):
    """
    Adds a "nearest node" column to a passed dataframe with geometries

    Parameters
    --------------------------------
    gdf : Geo DataFrame
        containing 'geometry' col for nods

    Returns
    --------------------------------
    GDF
        copy of gdf, but with an extra row for nearest node on RoadMap
    """
    tqdm.pandas(desc="Finding nearest Nodes:")

    # with tqdm(total=len(gdf.index), desc="Finding nearest Nodes:") as pbar:
    gdf["nearest node"] = gdf.progress_apply(
        lambda row: getPoint(row.geometry, row["Unit Type"]), axis=1
    )

    return gdf


def getArrayDistToStation(df):
    """
    returns the distance to a previously set statation for a a passed dataframe
    requires a station be set.  May crash if no station is set.

    Parameters
    --------------------------------
    df : Dataframe
        should contain latitude and longitudes for each location

    Returns
    --------------------------------
    Dataframe
    """
    with tqdm(
        total=len(stationDict) * len(df.index), desc="Routing All Stations:"
    ) as stationBar:
        for curStat in stationDict:
            stationBar.update(1)
            # set station on road map
            setStation(stationDict[curStat])
            # calculate distances

            tqdm.pandas(
                desc=f"Calculating distance to {curStat}:",
                leave=False,
            )
            df["Distance to {0} in miles".format(curStat)] = df.progress_apply(
                lambda x: distToStationFromNode(
                    x["nearest node"],
                    stationBar,
                ),
                axis=1,
            )

    return df


def addClosestStations(df):
    import re  # make sure that we can run regular expressions.

    names = [f"Distance to {i} in miles" for i in stationDict]
    df["Closest Station"] = df[names].idxmin(axis=1)

    def tryRegex(x):
        try:
            return re.search("(?<=Distance to )(.*)(?= in miles)", x).group(0)
        except:
            return None

    # Simplify Name
    df["Closest Station"] = df.apply(
        lambda x: tryRegex(str(x["Closest Station"])),
        axis=1,
    )
    return df


# ##############################################################################################################################################
#     Map Processing Helper Functions
# ##############################################################################################################################################
def downloadData():
    place_name = "Pflugerville, Texas, United States"
    # buffer distance is area in meters outside of the city.
    # district can extend up to 5 miles out
    # 10000m = 6.21 miles
    G = ox.graph_from_place(place_name, buffer_dist=distBuf)
    # nx.set_edge_attributes(G, 100, "w3")

    # save graph to disk
    print("  Saving Downloaded Map ...")
    ox.save_graphml(G, "./data/roads/roads.graphml")
    print("  Save Complete!")

    # return the data
    return G


def simplifyMap(G):
    # Project the map (into the proper GPS coordinates system?)
    print(" projecting map...")
    GProj = ox.project_graph(G)

    print(" Consolidating...")
    GCon = ox.consolidate_intersections(
        GProj, rebuild_graph=True, tolerance=20, dead_ends=False
    )

    print("  Saving Simplified Map ...")
    ox.save_graphml(G, "./data/roads/roadsProjected.graphml")
    print("  Save Complete!")

    return GCon


def getRoads():
    global roadMap

    # Load the data if it exists, else fetch it amd process it
    if not exists("./data/roads/roadsProjected.graphml"):
        # final version does exists, see if partial one does.
        if not exists("./data/roads/roads.graphml"):
            print("Downloading data from the api, this may take a moment")
            G = downloadData()
        else:
            print("Found a partial file:")
            G = ox.load_graphml("./data/roads/roads.graphml")
        # then prep for final data
        GCon = simplifyMap(G)
    else:
        print("Completed Map Exists, this will be quite quick")
        G = ox.load_graphml("./data/roads/roadsProjected.graphml")
        print(" projecting map...")
        GProj = ox.project_graph(G)

        print(" Consolidating...")
        GCon = ox.consolidate_intersections(
            GProj, rebuild_graph=True, tolerance=5, dead_ends=False
        )

    print("Projecting to Texas Local Map...")
    GFIPS = ox.project_graph(GCon, to_crs="epsg:2277")

    print("Map is ready for use!")

    # store andreturn the data
    roadMap = GFIPS
    return GFIPS


# =================================
#    Primary function called from outside
# =================================


def addRoadDistances(df):
    # df["Closest Station"] = None
    # return df

    import re
    import getData as data

    global stationDict
    stationDict = data.getStations()

    # add geometry to map, and convert to FIPS
    geometry = [Point(xy) for xy in zip(df["X-Long"], df["Y_Lat"])]
    gdf = GeoDataFrame(df, crs="EPSG:4326", geometry=geometry)
    gdf = gdf.to_crs(2277)

    # Load road map data
    getRoads()

    gdf = addNearestNodeToGDF(gdf)

    gdf = getArrayDistToStation(gdf)

    # these dont really mean anything without the context of the graph, so drop them off...
    df1 = pd.DataFrame(gdf.drop(columns=["geometry", "nearest node"]))

    # TODO: add Closest Station column
    df1 = addClosestStations(df1)
    return df1


################################
# ==================================================================
#
#
# Testing Code: will only run when this file is called directly.
# ==================================================================
################################


def testMap():
    # load road data
    roads = getRoads()

    print("graph is complete.  Setting Station Location")

    setStation(stationDict["S1"])

    print("finding distances:")
    # gps points to 800 Cheyenne Valley Cove, Round Rock, TX 78664.  Distance is off by ~5%
    # (7659.590000000004), but google claims 8.0km
    dist = distToStationFromGPS(30.496659877487765, -97.60270496406959)
    print("Distance is ", dist)

    print("Distances Calculated.  Opening view.")
    # plot graph
    fig, ax = ox.plot_graph(roads)
    plt.tight_layout()


def testNearestNodes():
    from pandasgui import show

    import loadTestFile

    df = loadTestFile.get()
    # df = df.head(50)
    # remove data not useful for the testing
    limit = [
        "Y_Lat",
        "X-Long",
    ]
    # df = df[limit]

    geoTime = Timer("Generating Points")
    geoTime.start()

    geometry = [Point(xy) for xy in zip(df["X-Long"], df["Y_Lat"])]
    gdf = GeoDataFrame(df, crs="EPSG:4326", geometry=geometry)

    geoTime.stop()

    ##
    t1 = Timer()
    t1.start()
    gdf = gdf.to_crs(2277)
    t1.end()

    t2 = Timer("Load Map")
    t2.start()
    getRoads()
    t2.end()

    t3 = Timer("Add Nearest Node to GDF")
    t3.start()
    # add_nearest_node_to_gdf(gdf, roads)
    gdf = addNearestNodeToGDF(gdf)
    t3.end()

    t4 = Timer("Finding Distance to Station")
    t4.start()
    gdf = getArrayDistToStation(gdf)
    t4.end()

    show(gdf)


def testStandAlone():
    import loadTestFile
    from pandasgui import show

    df = loadTestFile.get()
    df = df.head(150)
    gdf = addRoadDistances(df)
    show(gdf)


def main():
    # testMap()
    # testNearestNodes()
    testStandAlone()


if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
