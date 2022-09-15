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

## Installing Esri Overwritter

### PreReq

install KERBEROS
https://web.mit.edu/KERBEROS/dist/

### Compile Code

run the following line in the root of this folder:

```
pyinstaller esriOverwrite.spec
```
