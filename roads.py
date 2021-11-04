import pyproj
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import transform
import osmnx as ox
from osmnx import distance as dist
import networkx as nx
import matplotlib.pyplot as plt
from os.path import exists

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

    print("===== Station node:distance - \n", nodeDist)
    print(point)
    print(roadMap.nodes[dest_node], "\n===============================\n")

    # dest_node = ox.nearest_nodes(roadMap, lon, lat)

    # print(station, ":", dest_node)

    try:
        # how long is our route in meters?
        # dist = ox.shortest_path(roadMap, station, dest_node, weight="length")
        dist = nx.shortest_path_length(roadMap, station, dest_node, weight="length")
        # print("shortest path is:", dist)
    except:
        print(
            "You have just attempted to find the distance from a station, without first setting a station (setStation(lat,lon))"
        )

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
    ox.save_graphml(G, "./data/roads/roads.graphml")

    # return the data
    return G


def getRoads():
    # Load the data if it exists, else fetch it first
    if not exists("./data/roads/roads.graphml"):
        print("downloading data from the api, this may take a moment")
        G = downloadData()
    else:
        print("loading data from file.  This should be quite quick")
        G = ox.load_graphml("./data/roads/roads.graphml")

    # Project the map (into the proper GPS coordinates system?)
    print("projecting map...")
    GProj = ox.project_graph(G)

    print("Consolidating")
    GCon = ox.consolidate_intersections(
        GProj, rebuild_graph=True, tolerance=20, dead_ends=False
    )

    print("projecting to 2277")
    GFIPS = ox.project_graph(GCon, to_crs="epsg:2277")

    # store andreturn the data
    global roadMap
    roadMap = GFIPS
    return GFIPS


# Testing Code: will only run when this file is called directly.
# ==================================================================


def main():
    # load road data
    roads = getRoads()

    print("graph is complete.  Setting Station Location")

    # create temporary dictionary of stations
    #               "station name" = [lat, lon]
    stationDict = {"S1": [30.438998418785996, -97.61916191173464]}

    # print(stationDict["S1"])
    setStation(stationDict["S1"])

    print("finding distances:")
    # gps points to 800 Cheyenne Valley Cove, Round Rock, TX 78664.  Distance is off by ~5%
    # (7659.590000000004), but google claims 8.0km
    # attempting again after projecting map... - it now shows 0.  Great
    dist = getDistToStation(30.496659877487765, -97.60270496406959)
    print("Distance is ", dist)

    print("Distances Calculated.  Opening view.")
    # plot graph
    fig, ax = ox.plot_graph(roads)
    plt.tight_layout()


if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
