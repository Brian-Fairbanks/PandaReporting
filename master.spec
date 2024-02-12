# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

added_files = [('.env', '.'), ('*.py', '.')]
hidden_imports = ["requests_ntlm", "arcgis", "pyodbc", "sqlalchemy", "dotenv", "tqdm", "tabulate", "geopandas", "fiona", "shapely", "shapely.geometry", "fiona._shim", "fiona.schema", "osmnx", "networkx", "easygui", "email.mime.multipart", "email.mime.text", "email.mime.application"]

a = Analysis(['gui.py', 'AutoImportFromFTP.py', 'esriOverwrite.py'],
             pathex=[],
             binaries=[],
             datas=added_files,
             hiddenimports=hidden_imports,
             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=['arcpy'],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)

# Add additional data files
a.datas += Tree('./data/Lists', prefix='data/Lists')
a.datas += Tree('./Shape', prefix='Shape')
a.datas += Tree('./reports', prefix='reports')
a.datas += Tree('./osmnx', prefix='osmnx')
a.datas += Tree('./arcgis', prefix='arcgis')

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# Create separate executables for each script
exe_gui = EXE(pyz,
              a.scripts + [('gui.py', 'gui.py', 'PYSOURCE')],
              [],
              exclude_binaries=True,
              name='gui',
              debug=False,
              bootloader_ignore_signals=False,
              strip=False,
              upx=True,
              console=True,
              disable_windowed_traceback=False)

exe_ftp = EXE(pyz,
              a.scripts + [('AutoImportFromFTP.py', 'AutoImportFromFTP.py', 'PYSOURCE')],
              [],
              exclude_binaries=True,
              name='AutoImportFromFTP',
              debug=False,
              bootloader_ignore_signals=False,
              strip=False,
              upx=True,
              console=True,
              disable_windowed_traceback=False)

exe_esri = EXE(pyz,
               a.scripts + [('esriOverwrite.py', 'esriOverwrite.py', 'PYSOURCE')],
               [],
               exclude_binaries=True,
               name='esriOverwrite',
               debug=False,
               bootloader_ignore_signals=False,
               strip=False,
               upx=True,
               console=True,
               disable_windowed_traceback=False)

# Collect all necessary binaries, zip files, and data files into one folder
coll = COLLECT(exe_gui,
               exe_ftp,
               exe_esri,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=[],
               name='AllExecutables')
