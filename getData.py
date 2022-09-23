import json


def load(file):
    with open(f"./data/Lists/{file}.json") as json_file:
        return json.load(json_file)


def getStations():
    return load("stations")


def getLocations():
    return load("locations")


def getReserves():
    return load("reserves")


def getSpecialUnits():
    return load("units")


def main():
    print(getLocations())


if __name__ == "__main__":
    main()
