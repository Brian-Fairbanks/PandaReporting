import json
from os import path
import ServerFiles as sf

# Setup base directory
base_dir = sf.get_base_dir()

def load(file):
    with open(path.join(base_dir, "data", "Lists", f"{file}.json")) as json_file:
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
