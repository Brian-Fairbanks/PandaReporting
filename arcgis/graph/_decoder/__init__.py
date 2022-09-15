import sys

_py_version = sys.version_info

if _py_version.minor == 7:
    from arcgis.graph._decoder.for_python37 import _arcgisknowledge
if _py_version.minor == 8:
    from arcgis.graph._decoder.for_python38 import _arcgisknowledge
if _py_version.minor == 9:
    from arcgis.graph._decoder.for_python39 import _arcgisknowledge
