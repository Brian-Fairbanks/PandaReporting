# INSERT INTO table_name (column1, column2, column3, ...)
# VALUES (value1, value2, value3, ...);

import csv

openFile = open("C:/Users/bfairbanks/Desktop/FIRE ESD02_2006_2011 - Copy.csv", "r")
csvFile = csv.reader(openFile)
header = next(csvFile)
# headers = map((lambda x: '`'+x+'`'), header)
headers = [
    "sequence",
    "year",
    "status",
    "master_incident_number",
    "calltaker_agency",
    "address_of_incident",
    "city",
    "jurisdiction",
    "response_area",
    "problem",
    "incident_type",
    "response_plan",
    "priority_description",
    "alarm_level",
    "map_info",
    "x_long",
    "y_lat",
    "esd02_shift",
    "earliest_time_phone_pickup_afd_or_ems",
    "incident_time_call_entered_in_queue",
    "time_first_assigned",
    "time_first_enroute",
    "incident_time_first_staged",
    "time_first_arrived",
    "incident_time_call_closed",
    "last_unit_clear_incident",
    "earliest_time_phone_pickup_to_in_queue",
    "in_queue_to_first_assigned",
    "earliest_time_phone_pickup_to_first_assigned",
    "incident_turnout_firstassigned_to_first_enroute",
    "incident_travel_time_firstenroute_to_first_arrived",
    "incident_first_unit_response_first_assigned_to_first_arrived",
    "time_spent_onscene_first_arrived_to_lastcall_cleared",
    "earliest_time_phone_pickup_to_first_arrived",
    "incident_duration_earliest_time_phone_pickup_to_lastcall_cleared",
    "incident_call_disposition",
    "incident_call_reason",
    "ems_incident_numbers",
    "radio_name",
    "department",
    "frontline_status",
    "location_at_assign_time",
    "first_assign",
    "first_arrived",
    "unit_time_assigned",
    "unit_time_enroute",
    "unit_time_staged",
    "unit_time_arrived_at_scene",
    "unit_time_call_cleared",
    "in_queue_to_unit_dispatch",
    "unit_dispatch_to_respond_time",
    "unit_respond_to_arrival",
    "unit_dispatch_to_onscene",
    "unit_onscene_to_clear_call",
    "earliest_phone_pickup_time_to_unit_arrival",
    "unit_assign_to_clear_call_time",
    "unit_call_disposition",
    "unit_cancel_reason",
]

insert = "INSERT INTO fire_incidents (" + ", ".join(headers) + ") VALUES "

f = open("insertStatements.txt", "w")
for row in csvFile:
    values = map((lambda x: '"' + x + '"'), row)
    f.write(insert + "(" + ", ".join(values) + ");\n")
f.close()
openFile.close()
