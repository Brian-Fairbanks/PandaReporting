import geopandas as gpd
from shapely.geometry import Point
import osmnx as ox
import networkx as nx
import matplotlib.pyplot as plt
from os.path import exists


def downloadData():
    place_name = "Pflugerville, Texas, United States"
    G = ox.graph_from_place(place_name)
    nx.set_edge_attributes(G, 100, "w3")

    # save graph to disk
    ox.save_graphml(G, "./data/roads/roads.graphml")

    # and return the data
    return G


def getRoads():
    # # Get Road Bondaries, to limit how much data needs to be read in
    # ##############################################################

    # bounds = gpd.read_file("Shape\\roadBounds.shp")
    # # specify that source data is 'NAD 1983 StatePlane Texas Central FIPS 4203 (US Feet)' - https://epsg.io/2277
    # bounds.set_crs(epsg=2277, inplace=True)
    # # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    # bounds = bounds.to_crs(4326)

    # Load the data if it exists, else fetch it first
    if not exists("./data/roads/roads.graphml"):
        print("downloading data from the api, this may take a moment")
        graph = downloadData()
    else:
        print("loading data from file.  This should be quite quick")
        graph = ox.load_graphml("./data/roads/roads.graphml")

    return graph


# Testing Code: will only run when this file is called directly.
# ==================================================================
def main():
    # load road data
    roads = getRoads()

    # plot it for reference
    fig, ax = ox.plot_graph(roads)
    plt.tight_layout()


if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
