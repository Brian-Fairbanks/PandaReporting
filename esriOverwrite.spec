# -*- mode: python ; coding: utf-8 -*-


block_cipher = None

added_files = [('.env', '.'),('*.py','.')]
a = Analysis(['esriOverwrite.py'],
             pathex=[],
             binaries=[],
             datas=added_files,
             hiddenimports=["requests_ntlm","arcgis", "pyodbc", "sqlalchemy", 'dotenv', 'tqdm'],
             hookspath=[],
             hooksconfig={},
            runtime_hooks=[],
            excludes=['arcpy'],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)
a.datas += Tree('./arcgis', prefix='arcgis')
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)

exe = EXE(pyz,
          a.scripts, 
          [],
          exclude_binaries=True,
          name='esriOverwrite',
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
               name='esriOverwrite')
