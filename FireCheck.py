import utils


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
def check0(fireDF):
    c0 = fireDF[(fireDF["Earliest Time Phone Pickup AFD or EMS"].isnull())]
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
        utils.pprint(c0[limit])
        return c0[limit]
    return None


def main():
    pass


if __name__ == "__main__":
    main()
