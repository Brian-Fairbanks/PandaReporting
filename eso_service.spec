# -*- mode: python ; coding: utf-8 -*-

import os

block_cipher = None

# Add missing custom modules explicitly if they are not being detected.
hidden_imports = [
    'win32timezone',
    'pyodbc',
    'sqlalchemy'
]

a = Analysis(['eso_windows_service_wrapper.py'],
             pathex=[os.path.abspath('.')],
             binaries=[],
             datas=[(".env", ".")],  # Include only necessary data files
             hiddenimports=hidden_imports,
             hookspath=[],
             runtime_hooks=[],
             excludes=['matplotlib', 'PyQt5', 'pandasgui', 'plotly', 'IPython', 'jupyter', 'notebook', 'nbconvert', 'jedi', 'tk', 'Tornado','tornado'],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher,
             noarchive=False)

pyz = PYZ(a.pure, a.zipped_data,
          cipher=block_cipher)

exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='eso_service',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=True,  # Set to True if you need a console window for debugging
          )

coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               name='eso_service')
