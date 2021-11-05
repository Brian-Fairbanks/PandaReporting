import pyproj
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import transform
import osmnx as ox
from osmnx import distance as dist
import networkx as nx
import matplotlib.pyplot as plt
from os.path import exists
import traceback

station = ""
roadMap = ""


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

    print("===== Station node:distance - \n", nodeDist)
    print(point)
    print(roadMap.nodes[node], "\n===============================\n")

    station = node
    return station


def getArrayDistToStation(df):
    """
    STUBBED - get back to this, as this will almost certainly run a LOT faster when vectorized with pandas
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


def getDistToStation(lat, lon):
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
    global station
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


def downloadData():
    place_name = "Pflugerville, Texas, United States"
    # buffer distance is area in meters outside of the city.
    # district can extend up to 5 miles out
    # 10000m = 6.21 miles
    G = ox.graph_from_place(place_name, buffer_dist=10)
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
        GCon = ox.load_graphml("./data/roads/roadsProjected.graphml")

    print("Projecting to Texas Local Map...")
    GFIPS = ox.project_graph(GCon, to_crs="epsg:2277")

    print("Map is ready for use!")

    # store andreturn the data
    roadMap = GFIPS
    return GFIPS


# Testing Code: will only run when this file is called directly.
# ==================================================================

# create temporary dictionary of stations
#               "station name" = [lat, lon]
stationDict = {"S1": [30.438998418785996, -97.61916191173464]}


def testMap():
    # load road data
    roads = getRoads()

    print("graph is complete.  Setting Station Location")

    setStation(stationDict["S1"])

    print("finding distances:")
    # gps points to 800 Cheyenne Valley Cove, Round Rock, TX 78664.  Distance is off by ~5%
    # (), but google claims 8.0km
    dist = getDistToStation(30.496659877487765, -97.60270496406959)
    print("Distance is ", dist)

    print("Distances Calculated.  Opening view.")
    # plot graph
    fig, ax = ox.plot_graph(roads)
    plt.tight_layout()


def testNearestNodes():

    roads = getRoads()
    setStation(stationDict["S1"])


def main():
    testMap()
    # testNearestNodes()


if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
