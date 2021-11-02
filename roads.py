import geopandas as gpd


def getRoads():
    roads = gpd.read_file("austin_texas.geojson")
    roads.shape
    return roads


def main():
    roads = getRoads()
    print("got roads.")
    # roads.plot()


if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
