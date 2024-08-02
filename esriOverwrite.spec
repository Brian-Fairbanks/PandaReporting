# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

added_files = [(".env", ".")]
hidden_imports = [
    "requests_ntlm",
    "arcgis",
    "arcgis.gis",
    "arcgis.gis._impl._portalpy",
    "arcgis.auth.tools._lazy",
    "pyodbc",
    "sqlalchemy",
    "dotenv",
    "tqdm",
    "charset_normalizer",
    "pandas",
    "osgeo"
]



a = Analysis(
    ["esriOverwrite.py"],
    pathex=['.'],
    binaries=[],
    datas=added_files,
    hiddenimports=hidden_imports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    cipher=block_cipher,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    exclude_binaries=True,
    name='esriOverwrite',
    debug=False,
    strip=False,
    upx=True,
    console=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='esriOverwrite',
)
