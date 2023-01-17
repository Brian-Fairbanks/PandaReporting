from requests import get as getRequest
from dotenv import load_dotenv
from time import time
from os import getenv
import pandas as pd

import logging

logger = logging.getLogger(__name__)
print = logger.info

load_dotenv()

# api-endpoint
URL = "https://maps.googleapis.com/maps/api/geocode/json"
PARAMS = {
    "key": getenv("APIKEY"),
    "address": None,
}


def addCoordinates(df, progress=None):
    """Return a passed dataframe with GPS coordinates added.
    ----------
    df : DataFrame
        a dataframe which contain an address column.
    """

    print("Beginning calls to Google:")

    hashAddress = {}

    def doesNeed(row, lon, lat, progress):
        address = row["Address of Incident"]

        # geocode as little as possible.  log addresses as they are searched
        if address in hashAddress.keys():
            print(f"{address} found in hash.")
            return hashAddress[address]

        # return lat and lon if they already exist
        if not pd.isnull(lat) and not pd.isnull(lon):
            if lat != 0 and lon != 0:
                return f"{lat}, {lon}"

        # get coords and return them
        try:
            print(f"I'll have to look up {address}")
            coords = getCoordinates(address, progress)
            print(f"  -  {coords}")
            hashAddress[address] = coords
        except Exception as e:
            print(e)
            return f"{lat}, {lon}"
        return coords

    df["GPS"] = df.apply(
        lambda row: doesNeed(row, row["X-Long"], row["Y_Lat"], progress),
        axis=1,
    )

    errors = {}

    def getCoord(row, pos):
        gps = row["GPS"]
        # print(f"processing {gps}")
        try:
            return gps.split(",")[pos]
        except Exception as e:
            # errors[row["Sr No"]] = e
            print(e)
            return None

    df["Y_Lat"] = df.apply(lambda row: getCoord(row, 0), axis=1)
    df["X-Long"] = df.apply(lambda row: getCoord(row, 1), axis=1)
    df = df.drop(["GPS"], axis=1)

    # print(errors)
    print("Complete")

    return df


def getCoordinates(address, progress={}):
    failure = "30.400000, -97.600000"
    # throw an error if address is invalid
    if address is None:
        print(" - Geocoding Failed")
        return failure
    # progress["value"] += 1
    print(f"\tObtaining GPS for: {address}")
    # Rate Limiting requests
    start, end = time(), time()
    PARAMS["address"] = address

    try:
        r = getRequest(url=URL, params=PARAMS)
        loc = r.json()["results"][0]["geometry"]["location"]

        # get coordinate function must always take 1 second by rules of api
        while end - start < 1:
            end = time()

        # return time()
        return f'{loc["lat"]},{loc["lng"]}'
    except:
        print(" - Geocoding Failed")
        return failure


def fixCoords(df):
    from pandasgui import show

    print("- Correcting bad GPS Coordinates")

    # badGPS = df.query("`X-Long` == 0 or `Y_Lat` == 0")
    # show(badGPS)

    df = addCoordinates(df, {})
    show(df)


if __name__ == "__main__":
    getCoordinates(
        "911 West Pflugerville Parkway, Pflugerville, TX, Travis County, USA, 78660"
    )
