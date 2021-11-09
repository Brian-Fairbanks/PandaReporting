from tabulate import tabulate


def pprint(dframe):
    print(tabulate(dframe, headers="keys", tablefmt="psql", showindex=False))


def sort(fireDF):
    fireDF = fireDF.sort_values(
        by=[
            "Master Incident Number",
            "Unit Time Arrived At Scene",
            "Unit Time Staged",
            "Unit Time Enroute",
            "Unit Time Assigned",
        ]
    )
    fireDF = fireDF.reset_index(drop=True)
    return fireDF


# =================================================================
#     Check # 0 -  Checking for misssing "Earliest Time Phone Pickup AFD or EMS"
# =================================================================

# If any case where 'Earliest Time Phone Pickup AFD or EMS' is blank, Either pull updated information from visinet, or copy time from 'Incident Time Call Entered in Queue'
def check0(df):
    c0 = df[(df["Earliest Time Phone Pickup AFD or EMS"].isnull())]
    if c0.size > 0:
        # limit the rows that will show in the error output
        limit = [
            "Master Incident Number",
            "Earliest Time Phone Pickup AFD or EMS",
            "Incident Time Call Entered in Queue",
        ]
        print(
            "Warning: Please check on the following incidents:\n 'Earliest Time Phone Pickup AFD or EMS' is blank \n 'Earliest Time Phone Pickup AFD or EMS' field must have a value to continue\n Either pull updated information from visinet, or copy time from 'Incident Time Call Entered in Queue' field \n\n"
        )
        pprint(c0[limit])
        return c0[limit]
    return None


# =================================================================
#     Check # 1 -  Checking for misssing first arrived status
# =================================================================

# its a problem if there is no FirstArrived
# check each incident in visinet - find Phone Pickup Time
def check1(df):
    c1 = df[(df["FirstArrived"].isnull())]
    c1 = c1[(c1["Unit Time Arrived At Scene"].notnull())]

    # and it is not an
    #     alarm test - ALARMT
    #     burn notification - CNTRL02
    #     test - TEST'
    c1 = c1[(~c1["Radio_Name"].isin(["ALARMT", "CNTRL02", "TEST"]))]

    if c1.size > 0:
        limit = [
            "Master Incident Number",
            "Unit Time Assigned",
            "Unit Time Enroute",
            "Unit Time Staged",
            "Unit Time Arrived At Scene",
            "Unit Time Call Cleared",
        ]
        print(
            "Warning: Please check on the following incidents:\n We arrived on scene, but first arrived is blank \n 'Earliest Time Phone Pickup AFD or EMS' field must have a value to continue \n\n"
        )
        pprint(c1[limit])
        return c1[limit]


# =================================================================
#     Check #2 -  Missing First Arrived Time
# =================================================================
def check2(df):
    c2 = df[(df["FirstArrived"] == "Yes")]
    c2 = c2[(c2["Unit Time Arrived At Scene"].isnull())]

    if c2.size > 0:
        print(
            "Warning: Please check on the following incidents:\nThese incidents are missing 'Unit Time Arrived At Scene' field \n 'Unit Time Arrived At Scene' field must have a value to continue \n\n",
            c2,
        )
        return c2


# =================================================================
#     Check #3 -  PhonePickupTime is  unknown*
# =================================================================
def check3(df):
    c3 = df[
        (df["Earliest Time Phone Pickup AFD or EMS"] == "Unknown")
        | (df["Earliest Time Phone Pickup AFD or EMS"].isnull())
    ]
    ###  more than likely TCSO or APD, but still has to be filled
    # c3 = c3[(~c3["Calltaker Agency"].isin(["TCSO", "APD"]))]

    if c3.size > 0:
        limit = [
            "Master Incident Number",
            "Earliest Time Phone Pickup AFD or EMS",
            "Unit Time Assigned",
            "Unit Time Enroute",
            "Unit Time Staged",
            "Unit Time Arrived At Scene",
            "Unit Time Call Cleared",
        ]
        print(
            "Warning: Please check on the following incidents:\n'Earliest Time Phone Pickup AFD or EMS' is blank or 'unknown':\n"
        )
        pprint(c3[limit])
        return c3[limit]


def main():
    pass


if __name__ == "__main__":
    main()
