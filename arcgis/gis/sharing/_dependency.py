from typing import Dict, Any
from arcgis.gis import GIS


class DependencyManager:
    """Provides the ability for Manager to Rebuild Item Dependencies"""

    _gis: GIS = None
    _urls: dict = None

    def __init__(self, gis: GIS):
        self._gis = gis
        isinstance(gis, GIS)

        self._urls = {
            "rebuild": f"{gis._portal.resturl}portals/self/rebuildDependencies",
            "status": f"{gis._portal.resturl}portals/self/rebuildDependencies/status",
            "stop": f"{gis._portal.resturl}portals/self/stopRebuildDependencies",
        }

    def rebuild(self) -> Dict[str, Any]:
        """
        Rebuilds all the Item Dependencies on the Enterprise

        :returns: Dict[str, Any]

        """
        url = self._urls["rebuild"]
        params = {"f": "json"}
        return self._gis._con.post(url, params)

    def status(self) -> Dict[str, str]:
        """
        Checks to see if the dependency graph database is rebuilding

        :returns: Dict[str,str]
        """
        url = self._urls["status"]
        params = {"f": "json"}
        return self._gis._con.get(url, params)

    def terminate(self):
        """
        Ends the rebuild of the dependency database.
        """
        url = self._urls["stop"]
        params = {"f": "json"}
        return self._gis._con.post(url, params)
