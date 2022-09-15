import datetime as _dt
from arcgis.auth.tools import LazyLoader


try:
    from arcgis.graph._decoder import _arcgisknowledge as _kgparser

    HAS_KG = True
except ImportError as e:
    HAS_KG = False

_gis = LazyLoader("arcgis.gis")
_isd = LazyLoader("arcgis._impl.common._isd")
from typing import List
import platform


class KnowledgeGraph:
    """
    Provides access to a Knowledge Graph's datamodel and properties, as well as
    methods to search and query the graph.

    ==================     ====================================================================
    **Argument**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Knowledge Graph URL
    ------------------     --------------------------------------------------------------------
    gis                    an authenticated :class:`arcigs.gis.GIS` object.
    ==================     ====================================================================

    .. code-block:: python

        # Connect to a Knowledge Graph:

        gis = GIS(url="url",username="username",password="password")

        knowledge_graph = KnowledgeGraph(url, gis=gis)

    """

    _gis = None
    _url = None
    _properties = None

    def __init__(self, url: str, *, gis=None):
        """initializer"""
        self._url = url
        self._gis = gis

    def _validate_import(self):
        if HAS_KG == False:
            raise ImportError(
                "An error occured with importing the KnowledgeGraph libraries. Please ensure you "
                "are using Python 3.7,3.8, or 3.9 on Windows or Linux platforms."
            )

    @classmethod
    def fromitem(cls, item):
        """Returns the KnowledgeGraph Service from an Item"""
        if item.type != "Knowledge Graph":
            raise ValueError(
                "Invalid item type, please provide a 'Knowledge Graph' item."
            )
        return cls(url=item.url, gis=item._gis)

    @property
    def properties(self) -> _isd.InsensitiveDict:
        """returns the properties of the service"""
        if self._properties is None:
            resp = self._gis._con.get(self._url, {"f": "json"})
            self._properties = _isd.InsensitiveDict(resp)
        return self._properties

    def search(self, search: str, category: str = "both") -> List[dict]:
        """
        Allows for the searching of the properties of entities,
        relationships, or both in the graph using a full-text index.

        `Learn more about searching a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-graph-search.htm>`_

        ================    ===============================================================
        **Argument**        **Description**
        ----------------    ---------------------------------------------------------------
        search              Required String. The search to perform on the knowledge graph.
        ----------------    ---------------------------------------------------------------
        category            Optional String.  The category is the location of the full
                            text search.  This can be isolated to either the `entities` or
                            the `relationships`.  The default is to look in `both`.

                            The allowed values are: both, entities, relationships
        ================    ===============================================================

        :return: List[list]

        """
        url = self._url + "/graph/search"
        cat_lu = {
            "both": _kgparser.esriNamedTypeCategory.both,
            "relationships": _kgparser.esriNamedTypeCategory.relationship,
            "entities": _kgparser.esriNamedTypeCategory.entity,
        }
        assert str(category).lower() in cat_lu.keys()
        r_enc = _kgparser.GraphSearchRequestEncoder()
        r_enc.search_query = search
        r_enc.return_geometry = True
        r_enc.max_num_results = self.properties["maxRecordCount"]
        r_enc.type_category_filter = cat_lu[category.lower()]
        r_enc.encode()
        assert r_enc.get_encoding_result().error.error_code == 0
        query_dec = _kgparser.GraphQueryDecoder()
        count = 0

        session = self._gis._con._session
        response = session.post(
            url=url,
            params={"f": "pbf"},
            data=r_enc.get_encoding_result().byte_buffer,
            stream=True,
            headers={"Content-Type": "application/octet-stream"},
        )
        rows = []
        query_dec = _kgparser.GraphQueryDecoder()
        query_dec.data_model = self._datamodel
        for chunk in response.iter_content(8192):
            did_push = query_dec.push_buffer(chunk)
            count = 0
            while query_dec.next_row():
                rows.append(query_dec.get_current_row())
                count += 1
        return rows

    def query(self, query: str) -> List[dict]:
        """
        Queries the Knowledge Graph using openCypher

        `Learn more about querying a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-graph-query.htm>`_

        ================    ===============================================================
        **Argument**        **Description**
        ----------------    ---------------------------------------------------------------
        query               Required String. Allows you to return the entities and
                            relationships in a graph, as well as the properties of those
                            entities and relationships, by providing an openCypher query.
        ================    ===============================================================

        :return: List[list]

        """
        self._validate_import()
        url = f"{self._url}/graph/query"
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
            "openCypherQuery": query,
        }

        data = self._gis._con.get(url, params, return_raw_response=True, try_json=False)
        buffer_dm = data.content
        gqd = _kgparser.GraphQueryDecoder()
        gqd.push_buffer(buffer_dm)
        gqd.data_model = self._datamodel
        rows = []
        while gqd.next_row():
            r = gqd.get_current_row()
            rows.append(r)
        return rows

    @property
    def _datamodel(self) -> object:
        """
        Returns the datamodel for the Knowledge Graph Service
        """
        self._validate_import()
        url = f"{self._url}/dataModel/queryDataModel"
        params = {
            "f": "pbf",
        }
        r_dm = self._gis._con.get(
            url, params=params, return_raw_response=True, try_json=False
        )
        buffer_dm = r_dm.content
        dm = _kgparser.decode_data_model_from_protocol_buffer(buffer_dm)
        return dm

    @property
    def datamodel(self) -> dict:
        """
        Returns the datamodel for the Knowledge Graph Service
        """
        self._validate_import()
        url = f"{self._url}/dataModel/queryDataModel"
        params = {
            "f": "pbf",
        }
        r_dm = self._gis._con.get(
            url, params=params, return_raw_response=True, try_json=False
        )
        buffer_dm = r_dm.content
        dm = _kgparser.decode_data_model_from_protocol_buffer(buffer_dm)
        return dm.to_value_object()
