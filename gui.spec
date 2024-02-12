# -*- mode: python ; coding: utf-8 -*-


block_cipher = None
added_files = [('.env', '.'),('*.py','.')]

a = Analysis(['gui.py'],
             pathex=[],
             binaries=[],
             datas=added_files,
             hiddenimports=['tabulate', 'tqdm', 'geopandas', 'fiona', 'shapely', 'shapely.geometry', 'fiona._shim', 'fiona.schema', 'osmnx', 'networkx', 'easygui', 'pyodbc', 'sqlalchemy', 'dotenv', 'email.mime.multipart', 'email.mime.text', 'email.mime.application'],

             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
a.datas += Tree('./data/Lists', prefix='data/Lists')
a.datas += Tree('./Shape', prefix='Shape')
a.datas += Tree('./reports', prefix='reports')
a.datas += Tree('./osmnx', prefix='osmnx')
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)


exe = EXE(pyz,
          a.scripts, 
          [],
          exclude_binaries=True,
          name='gui',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=True,
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas, 
               strip=False,
               upx=True,
               upx_exclude=[],
               name='gui')
