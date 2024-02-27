# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

added_files = [(".env", "."), ("*.py", ".")]
hidden_imports = [
    "requests_ntlm",
    "arcgis",
    "pyodbc",
    "sqlalchemy",
    "dotenv",
    "tqdm",
    "tabulate",
    "geopandas",
    "fiona",
    "shapely",
    "shapely.geometry",
    "fiona._shim",
    "fiona.schema",
    "osmnx",
    "networkx",
    "easygui",
    "email.mime.multipart",
    "email.mime.text",
    "email.mime.application",
]

executables_info = {
    "Gui": {
        "script": "gui.py",
        "additional_data": added_files,
        "hidden_imports": hidden_imports,
    },
    "AutoImportFromFTP": {
        "script": "AutoImportFromFTP.py",
        "additional_data": added_files,
        "hidden_imports": hidden_imports,
    },
    "esriOverwrite": {
        "script": "esriOverwrite.py",
        "additional_data": added_files,
        "hidden_imports": [
            "requests_ntlm",
            "arcgis",
            "pyodbc",
            "sqlalchemy",
            "dotenv",
            "tqdm",
        ],
    },
    "emailMonitor": {
        "script": "emailMonitor.py",
        "additional_data": added_files,
        "hidden_imports": ["dotenv"],
    },
}


exes = []
all_datas = set()
all_binaries = set()

for name, info in executables_info.items():
    analysis = Analysis(
        [info["script"]],
        datas=info["additional_data"]
        + [
            ("./data/Lists", "data/Lists"),
            ("./Shape", "Shape"),
            ("./reports", "reports"),
            ("./osmnx", "osmnx"),
            ("./arcgis", "arcgis"),
        ],
        hiddenimports=info["hidden_imports"],
        cipher=block_cipher,
    )

    pyz = PYZ(analysis.pure, analysis.zipped_data, cipher=block_cipher)

    exe = EXE(
        pyz,
        analysis.scripts,
        exclude_binaries=True,
        name=name,
        debug=False,
        console=True,
    )

    exes.append(exe)
    all_datas.update(analysis.datas)
    all_binaries.update(analysis.binaries)

coll = COLLECT(*exes, all_binaries, all_datas, name="AllExecutables")
