def getStep(time):
    if time <= 240:
        return "1-240"
    if time <= 390:
        return "241-390"
    if time <= 450:
        return "391-450"
    if time <= 600:
        return "451-600"
    if time <= 750:
        return "601-750"
    if time <= 600:
        return "Over 20 Min"
    return "Over 750"


def addPhPuSteps(df):
    df["Ph_PU2_UnitArrive Time_Intervals in seconds"] = df.apply(
        lambda row: getStep(row["Earliest Phone Pickup Time to Unit Arrival"]), axis=1
    )
    return df
