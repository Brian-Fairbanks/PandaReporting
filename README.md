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
