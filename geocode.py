from requests import get as getRequest
from dotenv import load_dotenv
from time import time
from os import getenv
import pandas as pd

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

    def doesNeed(row, lon, lat, progress):
        # Combine sections to get address
        addressComponents = [
            "Number Or Milepost",
            "Street Prefix",
            "Street Or Highway Name",
            "Street Type",
            "City",
            "State",
            "Zip",
        ]

        address = ""
        for part in addressComponents:
            if not pd.isnull(row[part]):
                address += str(row[part]) + " "

        if not pd.isnull(lat) and not pd.isnull(lon):
            return f"{lat}, {lon}"
        return getCoordinates(address, progress)

    df["GPS"] = df.apply(
        lambda row: doesNeed(row, row["Longitude"], row["Latitude"], progress),
        axis=1,
    )

    errors = {}

    def getCoord(row, pos):
        gps = row["GPS"]
        print(f"processing {gps}")
        try:
            return gps.split(",")[pos]
        except Exception as e:
            # errors[row["Sr No"]] = e
            print(e)
            return None

    df["Latitude"] = df.apply(lambda row: getCoord(row, 0), axis=1)
    df["Longitude"] = df.apply(lambda row: getCoord(row, 1), axis=1)
    df = df.drop(["GPS"], axis=1)

    # print(errors)
    print("Complete:  You may now close, or process a new file.")

    return df


def getCoordinates(address, progress={}):
    # throw an error if address is invalid
    if address is None:
        return None
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
        return None


if __name__ == "__main__":
    getCoordinates(
        "911 West Pflugerville Parkway, Pflugerville, TX, Travis County, USA, 78660"
    )
