# Panda_Reporting

Ensure you have installed Pandas, NumPy, and matplotlib

```
   pip install pandas
   pip install numpy
   pip install matplotlib
   pip install openpyxl
   pip install tabulate
   pip install xlsxwriter
   pip install shapely
   install included fiona.whl
   install included gdl.whl
   pip install geopandas
   pip install pywin32
   pip install pandasgui
   pip install easygui
   pip install pyodbc
   pip install sqlalchemy

   pip install arcgis
   pip install python-dotenv


```

Install MS Sql
schemas.sql included in root to set up the database

### Compile Code

run the following line in the root of this folder:

```
pyinstaller esriOverwrite.spec
```

<br>
<br>
<br>

# Installing Esri Overwritter

### PreReq

install KERBEROS
https://web.mit.edu/KERBEROS/dist/

### Compile Code

run the following line in the root of this folder:

```
pyinstaller esriOverwrite.spec
```

<br>
<br>
<br>

# Installing Google Form Reader

follow the guide outlined on [developers.google](https://developers.google.com/forms/api/quickstart/python)

```
pip install --upgrade google-api-python-client
```

- I had to run the following, but im not really sure why. It said everything was already installed when run, yet still resolved my issues.

```
pip install oauth2client
```

Dont forget to get the client-secret.json associated with the login account!  
It has been included in .gitignore

# Important Notes

for GPS coordinate failures, please note the following code...
`tcesd2_admin_office_gps = "30.439196854214842, -97.6199526861625"`


Station Assignment Methodology and Issues
Background
Until early May, station assignments for reserve units (as designated by the reserve JSON file) prioritized fire units.
The current approach was implemented shortly after Station 5 became operational, however, this method has caused issues, particularly for EMS vehicles.
Issues
1.	Incorrect Station Identification: The current method has led to misidentification of stations, especially for newer stations where vehicles are
often identified as being further down the same road as other stations. This has been a significant issue for Med211, a reserve unit that frequently gets misidentified as Station 1.
2.	False Positives in "Assigned_at_Station": Due to our recent re-prioritizing road names for EMS vehicles (which are less likely to change their operation names at the CAD level),
the "Assigned_at_Station" column is now unfortunately showing many false positives.
Updated Methodology
To improve accuracy, we intend to update the station assignment process with the following steps:
•	Prioritization of AFD Response Box: The AFD_Response_Box column will now be prioritized over street name/location , as it accurately defines the station response order since mid-2022.
•	Incident Location for Reserve Units: For reserve units not actively in a station, we will now prioritize the incident location over the street location.
•	GPS Coordinates for EMS Units: EMS units may use GPS coordinates to compare with station GPS coordinates. If they are very close, they may be assigned to that station.
Future Work
We are exploring more robust methods to accurately determine station assignments for both fire and EMS units, aiming to reduce misidentifications and false positives.

