import collections
from functools import wraps
from pathlib import Path
import re
from typing import Any, Union, Iterable, Optional

from arcgis import __version__
from arcgis import env
from arcgis.features import FeatureSet, GeoAccessor, GeoSeriesAccessor
from arcgis.geometry import Geometry, SpatialReference
from arcgis.gis import GIS
from arcgis._impl.common._deprecate import deprecated
from arcgis._impl.common._utils import _lazy_property
import pandas as pd

from . import _business_analyst
from ._business_analyst._utils import (
    local_vs_gis,
    local_business_analyst_avail,
    local_ba_data_avail,
    avail_arcpy,
)
from ._ge import _GeoEnrichment


def _check_gis_source(gis=None):
    """Helper function handling using GIS('pro') as local source."""
    # handle if GIS('pro') used to indicate using local source
    if isinstance(gis, GIS):
        if gis._con._auth == "PRO" or (gis._con._auth == "ANON" and avail_arcpy):
            local_ba_avail = local_business_analyst_avail() and local_ba_data_avail()
            assert local_ba_avail, (
                "If using ArcGIS Pro, you must have Business Analyst with at least one local data "
                "pack installed."
            )
            gis = "local"

    # if nothing else available, check to see if there is a gis in the session to use
    if gis is None and env.active_gis is not None:
        gis = env.active_gis

    return gis


def _call_method_by_source(fn) -> callable:
    """Function flow control based on gis or source - 'local' versus GIS object instance."""

    # get the method name - this will be used to redirect the function call
    fn_name = fn.__name__

    @wraps(fn)
    def wrapped(*args, **kwargs) -> Any:

        # try to pull out the source or gis caller
        src = None
        for param_src in ["source", "gis"]:
            if param_src in kwargs.keys():
                src = kwargs[param_src]
                break

        # if the source was not found in kwargs, look through the args
        if src is None:
            for p in args:
                if isinstance(p, str):
                    p = p.lower()
                    if p == "local":
                        src = p.lower()
                        break
                elif isinstance(p, GIS):
                    src = p
                    break

        # TODO: Swap the precedence of these once all methods are implemented
        # if the source is a GIS instance and was created using the "pro" keyword, set as local
        src = _check_gis_source(src)

        # make sure a source was located or bingo out
        assert src is not None, (
            "The gis parameter needs to be populated with a valid GIS instance since there is not an active GIS object "
            "in the session."
        )

        # build function name to call
        fn_nm_to_call = (
            f"_{fn_name}_gis" if isinstance(src, GIS) else f"_{fn_name}_local"
        )

        # get the function if it is implemented
        if fn_nm_to_call not in globals().keys():
            src_nm = (
                "Web GIS"
                if isinstance(src, GIS)
                else "local (ArcGIS Pro with Business Analyst)"
            )
            raise NotImplementedError(
                f"The {fn_name} function is not yet implemented with a {src_nm} source."
            )
        else:
            fn_to_call = globals()[fn_nm_to_call]

        # invoke the function and return the result
        return fn_to_call(*args, **kwargs)

    return wrapped


BufferStudyArea = collections.namedtuple(
    "BufferStudyArea", "area radii units overlap travel_mode"
)
BufferStudyArea.__new__.__defaults__ = (None, None, None, True, None)
BufferStudyArea.__doc__ = """BufferStudyArea allows you to buffer point and street address study areas.

Parameters:
area: the point geometry or street address (string) study area to be buffered
radii: list of distances by which to buffer the study area, eg. [1, 2, 3]
units: distance unit, eg. Miles, Kilometers, Minutes (when using drive times/travel_mode)
overlap: boolean, uses overlapping rings when True, or non-overlapping disks when False
travel_mode: None or string, one of the supported travel modes when using network service areas, eg. Driving, Trucking, Walking.
"""


def _pep8ify(name):
    """PEP8ify name"""
    if "." in name:
        name = name[name.rfind(".") + 1 :]
    if name[0].isdigit():
        name = "level_" + name
    name = name.replace(".", "_")
    if "_" in name:
        return name.lower()
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


class NamedArea(object):
    """
    Represents named geographical places in a country. Each named area has attributes for the
    supported subgeography levels within it, and the value of those attributes are dictionaries containing the named
    places within that level of geography. This allows for interactive selection of places using intellisense and a
    notation such as the following:

    .. code-block:: python

        # Usage Example

        usa = Country.get('USA')
        usa.subgeographies.states['California'].counties['San_Bernardino_County']

    """

    def __init__(self, country, name=None, level=None, areaid="01", geometry=None):
        self._gis = country._gis
        self._country = country
        self._currlvl = level
        self._areaid = areaid
        if geometry is not None:
            self.geometry = geometry
        self._name = country.properties.name
        self._level_mappings = {}

        for childlevel in self._childlevels:
            setattr(self, childlevel, None)

    @property
    def __studyarea__(self):
        return {
            "sourceCountry": self._country.properties.iso3,
            "layer": self._currlvl,
            "ids": [self._areaid],
        }

    def __str__(self):
        return '<%s name:"%s" area_id="%s", level="%s", country="%s">' % (
            type(self).__name__,
            self._name,
            self._areaid,
            self._currlvl,
            self._country.properties.name,
        )

    def __repr__(self):
        return '<%s name:"%s" area_id="%s", level="%s", country="%s">' % (
            type(self).__name__,
            self._name,
            self._areaid,
            self._currlvl,
            self._country.properties.name,
        )

    @property
    def _childlevels(self):
        dset = [
            dset
            for dset in self._country._geog_levels
            if dset["datasetID"] == self._country.dataset
        ][0]
        whole_country_levelid = [
            lvl["id"] for lvl in dset["levels"] if lvl["isWholeCountry"]
        ][0]
        if self._currlvl is None:
            self._currlvl = whole_country_levelid

        is_whole_country = self._currlvl == whole_country_levelid

        childlevels = set()
        for branch in dset["branches"]:

            levels = branch["levels"]
            if is_whole_country and self._currlvl not in levels:
                level_attr = _pep8ify(levels[0])
                childlevels.add(level_attr)
                self._level_mappings[level_attr] = levels[0]

            elif self._currlvl in levels:
                try:
                    nextlevel = levels[levels.index(self._currlvl) + 1]
                    level_attr = _pep8ify(nextlevel)
                    childlevels.add(level_attr)
                    self._level_mappings[level_attr] = nextlevel
                except IndexError:
                    # no nextlevel
                    pass
        return childlevels

    def __getattribute__(self, name):
        if not name.startswith("_") and not name in ["geometry"]:
            val = object.__getattribute__(self, name)
            if val is None:
                # print('Fetching {}'.format(name))
                self._fetch_subgeographies(name)
            return object.__getattribute__(self, name)

        else:
            return object.__getattribute__(self, name)

    def _fetch_subgeographies(self, name):
        df = standard_geography_query(
            source_country=self._country.properties.iso3,
            layers=[self._currlvl],
            ids=[self._areaid],
            return_sub_geography=True,
            sub_geography_layer=self._level_mappings[name],
            return_geometry=True,
            as_featureset=False,
            gis=self._country._gis,
        )

        places = {}
        for index, row in df.iterrows():
            #     print(dict(row))
            plc = dict(row)
            place = NamedArea(
                country=self._country,
                name=plc["AreaName"],
                level=plc["DataLayerID"],
                areaid=plc["AreaID"],
                geometry=Geometry(plc["SHAPE"]),
            )
            place_name = plc["AreaName"].replace(" ", "_")
            if self._level_mappings[name] == "US.ZIP5":
                place_name = plc["AreaID"]
            places[place_name] = place
        setattr(self, name, places)


class Country(object):
    """
    Enables access to data and methods for a specific country. This
    class can reference country data and methods available using data accessed through
    both a Web GIS and a local installation of `ArcGIS Pro with the Business Analyst
    extension and local country data` installed. Specifying this source is accomplished
    using the ``gis`` parameter when instantiating. If using the keyword ``Pro``,
    :class:`~arcgis.geoenrichment.Country` will use ArcGIS Pro with Business Analyst
    and will error if the specified country is not available locally. Available
    countries can be discovered using :func:`~arcgis.geoenrichment.get_countries`.
    """

    @classmethod
    def get(
        cls,
        name: str,
        gis: Optional[GIS] = None,
        year: Optional[Union[str, int]] = None,
    ):
        """
        Get a reference to a particular country, given its name, or its
        two letter abbreviation or three letter ISO3 code.

        ================  ========================================================
        **Argument**      **Description**
        ----------------  --------------------------------------------------------
        name              Required string. The country name, two letter code or
                          three letter ISO3 code identifying the country.
        ----------------  --------------------------------------------------------
        gis               Optional :class:`~arcgis.gis.GIS` instance. This
                          specifies what GIS country sources are available based
                          on the GIS source, a Web GIS (either `ArcGIS Online` or
                          `ArcGIS Enterprise`) or `ArcGIS Pro with the Business
                          Analyst extension and at least one country data pack`.
                          If not explicitly specified, it tries to use an active
                          GIS already created in the Python session. If an active
                          GIS is not available, it then tries to use local
                          resources, ArcGIS Pro with Business Analyst and at least
                          one country dataset installed locally. Finally, if
                          neither of these (Pro or an active GIS) are available,
                          a :class:`~arcgis.gis.GIS` object instance must be
                          explicitly provided.
        ----------------  --------------------------------------------------------
        year              Optional integer. Explicitly specifying the vintage
                          (year) of data to use. This option is only available
                          when using a `'local'` GIS source, and will be
                          ignored if used with a Web GIS source.
        ================  ========================================================

        :return:
            :class:`~arcgis.geoenrichment.Country` instance for the requested country.
        """
        return cls(name, gis, year)

    # noinspection PyMissingConstructor
    def __init__(
        self,
        iso3: str,
        gis: GIS = None,
        year: Optional[Union[str, int]] = None,
        **kwargs,
    ) -> None:

        # handle the caveat of using a GIS('Pro') input
        gis = _check_gis_source(gis)

        # instantiate a BA object instance and save for future
        ba = _business_analyst.BusinessAnalyst(gis)

        # pull the source out of the ba object since it takes care of all defaults and validation
        self._gis = ba.source

        # stash for use later
        self._ba_cntry = ba.get_country(iso3, year=year)

        # if the source is a GIS set a few more properties
        if isinstance(self._gis, GIS):

            # get the helper services to work with
            hlp_svcs = self._gis.properties["helperServices"]

            # legacy parameter support
            if "purl" in kwargs:
                self._base_url = kwargs["purl"]

            # otherwise, get the url if available
            else:
                assert (
                    "geoenrichment" in hlp_svcs.keys()
                ), "Geoenrichment does not appear to be configured for your portal."
                self._base_url = hlp_svcs["geoenrichment"]["url"]

            # if a hosted notebook environment, get the private service url
            if gis._is_hosted_nb_home:
                res = self._gis._private_service_url(self._base_url)
                prv_url = (
                    res["privateServiceUrl"]
                    if "privateServiceUrl" in res
                    else res["serviceUrl"]
                )
                self._base_url = prv_url

            # set the dataset_id to the default
            self._dataset_id = self._ba_cntry.properties.default_dataset

    def __repr__(self):
        if self._gis == "local":
            repr = (
                f"<{type(self).__name__} - {self.properties.name} {self.properties.year} "
                f"({self._gis.__repr__()})>"
            )
        else:
            repr = f"<{type(self).__name__} - {self.properties.name} ({self._gis.__repr__()})>"
        return repr

    @property
    def properties(self):
        """
        Returns ``namedtuple`` of relevant properties based on the gis source.
        """
        return self._ba_cntry.properties

    @_lazy_property
    def geometry(self):
        """
        Returns a :class:`~arcgis.geometry.Polygon` object delineating the country's area.

        .. note::

            Currently this is only supported when using a Web GIS (ArcGIS Online
            or ArcGIS Enterprise) as the ``gis`` source.

        """
        if isinstance(self._gis, GIS):
            lvlid = [lvl["id"] for lvl in self.levels if lvl["isWholeCountry"]][0]
            df = standard_geography_query(
                source_country=self.properties.iso2,
                layers=[lvlid],
                ids=["01"],
                return_sub_geography=False,
                return_geometry=True,
                as_featureset=False,
                gis=self._gis,
            )
            geom = Geometry(df.iloc[0]["SHAPE"])

        else:
            raise NotImplementedError(
                f"'geometry' not available using 'local' as the source."
            )

        return geom

    @_lazy_property
    def _geog_levels(self):
        """
        Returns levels of geography in this country, including branches for all datasets.
        """
        params = {"f": "json"}
        url = self._base_url + "/Geoenrichment/standardgeographylevels/%s" % (
            self.properties.iso2
        )
        res = self._gis._con.post(url, params)
        return res["geographyLevels"][0]["datasets"]

    @property
    def levels(self) -> pd.DataFrame:
        """
        Returns levels of geography in this country, for the current dataset.
        """
        return self._ba_cntry.geography_levels

    def _levels_gis(self):
        """GIS levels implementation."""
        dset = [d for d in self._geog_levels if d["datasetID"] == self._dataset_id][0]
        lvls = dset["levels"]
        return lvls

    @property
    def dataset(self):
        """
        Returns the currently used dataset for this country.json.

        .. note::

            This is only supported when using a Web GIS (ArcGIS Online
            or ArcGIS Enterprise as the ``gis`` source.

        """
        if isinstance(self._gis, GIS):
            ds_id = self._dataset_id
        else:
            raise NotImplementedError(
                f"'dataset' not available using 'local' as the source."
            )
        return ds_id

    @dataset.setter
    def dataset(self, value):
        if isinstance(self._gis, GIS):
            if value in self.properties.datasets:
                self._dataset_id = value
                try:
                    delattr(self, "_lazy_subgeographies")
                    delattr(self, "_lazy__geog_levels")
                except:
                    pass
            else:
                raise ValueError(
                    "The specified dataset is not available in this country. Choose one of "
                    + str(self.properties.datasets)
                )
        else:
            raise NotImplementedError(
                f"'dataset' not available using 'local' as the source."
            )

    @_lazy_property
    @local_vs_gis
    def data_collections(self):
        """
        Returns the supported data collections and analysis variables as a Pandas dataframe.

        The dataframe is indexed by the data collection id(``dataCollectionID``) and
        contains columns for analysis variables(``analysisVariable``).
        """
        pass

    def _data_collections_gis(self):
        """GIS implementation of data_collections"""
        import pandas as pd

        df = pd.json_normalize(
            (
                _data_collections(
                    country=self.properties.iso2,
                    out_fields=[
                        "id",
                        "dataCollectionID",
                        "alias",
                        "fieldCategory",
                        "vintage",
                    ],
                )
            )["DataCollections"],
            "data",
            "dataCollectionID",
        )
        df["analysisVariable"] = df["dataCollectionID"] + "." + df["id"]
        df = df[
            [
                "dataCollectionID",
                "analysisVariable",
                "alias",
                "fieldCategory",
                "vintage",
            ]
        ]
        df.set_index("dataCollectionID", inplace=True)
        return df

    def _data_collections_local(self):
        """Local implementation for data_collections"""
        # get the variables and reorganize the dataframe to be as similar as possible to the existing online response
        col_map = {
            "data_collection": "dataCollectionID",
            "enrich_name": "analysisVariable",
        }
        dc_df = self._ba_cntry.enrich_variables.rename(columns=col_map).set_index(
            "dataCollectionID"
        )
        dc_df = dc_df[["analysisVariable", "alias"]].copy()
        return dc_df

    @property
    def enrich_variables(self):
        """
        Pandas Dataframe of available geoenrichment variables.

        For instance, the following code, if run in Jupter, will render the table below.

        .. code-block:: python

            from arcgis.gis import GIS
            from arcgis.geoenrichment import Country

            usa = Country('usa', gis=GIS('pro'))

            usa.enrich_variables.head()

        .. list-table::
            :widths: 1 10 27 13 24 24
            :header-rows: 1

            * -
              - name
              - alias
              - data_collection
              - enrich_name
              - enrich_field_name
            * - 0
              - CHILD_CY
              - 2021 Child Population
              - AgeDependency
              - AgeDependency.CHILD_CY
              - AgeDependency_CHILD_CY
            * - 1
              - WORKAGE_CY
              - 2021 Working-Age Population
              - AgeDependency
              - AgeDependency.WORKAGE_CY
              - AgeDependency_WORKAGE_CY
            * - 2
              - SENIOR_CY
              - 2021 Senior Population
              - AgeDependency
              - AgeDependency.SENIOR_CY
              - AgeDependency_SENIOR_CY
            * - 3
              - CHLDDEP_CY
              - 2021 Child Dependency Ratio
              - AgeDependency
              - AgeDependency.CHLDDEP_CY
              - AgeDependency_CHLDDEP_CY
            * - 4
              - AGEDEP_CY
              - 2021 Age Dependency Ratio
              - AgeDependency
              - AgeDependency.AGEDEP_CY
              - AgeDependency_AGEDEP_CY

        The values in this table can be useful for filtering to identify the variables you
        want to use for analysis and also for matching to existing datasets. This table,
        once filtered to variables of interest can be used as input directly into the
        :func:`~arcgis.geoenrichment.Country.enrich` method's ``enrich_variables``
        parameter.

        Also, the ``enrich_field_name`` column in this table corresponds to the field
        naming convention used in output from the Enrich Layer tool in ArcGIS Pro. This
        enables you to, with a little scripting, identify variables used in previous
        analysis.
        """
        return self._ba_cntry.enrich_variables

    def _get_enrich_variables_from_iterable(
        self, enrich_variables: Union[Iterable, pd.Series], **kwargs
    ) -> pd.DataFrame:
        """
        Get a dataframe of enrich enrich_variables associated with the list of enrich_variables
        passed in. This is especially useful when needing aliases (*human readable
        names*), or are interested in enriching more data using previously enriched
        data as a template.

        ============================     ====================================================================
        **Argument**                     **Description**
        ----------------------------     --------------------------------------------------------------------
        enrich_variables                 Iterable (normally a list) of enrich_variables correlating to
                                         enrichment enrich_variables. These variable names can be simply the
                                         name, the name prefixed by the collection separated by a dot, or
                                         the output from enrichment in ArcGIS Pro with the field name
                                         modified to fit field naming and length constraints.
        ============================     ====================================================================

        return:
            Pandas DataFrame of enrich enrich_variables with the different available aliases.
        """
        return self._ba_cntry.get_enrich_variables_from_iterable(enrich_variables)

    def enrich(
        self,
        study_areas: Union[pd.DataFrame, Iterable, Path],
        enrich_variables: Optional[Union[pd.DataFrame, Iterable]] = None,
        return_geometry: bool = True,
        standard_geography_level: Optional[Union[int, str]] = None,
        standard_geography_id_column: Optional[str] = None,
        proximity_type: Optional[str] = None,
        proximity_value: Optional[Union[float, int]] = None,
        proximity_metric: Optional[str] = None,
        output_spatial_reference: Union[int, dict, SpatialReference] = 4326,
        **kwargs,
    ):
        """
        Enrich provides access to a massive dataset describing exactly who people are
        in a geographic location. The most common way to delineate study_areas for
        enrichment is using polygons delineated areas, although points and lines can
        be used as well.

        When points or lines are provided, an area surrounding the geometries is used
        for enrichment. This area can be defined using additional parameters, but by
        default is one kilometer around the geometry. Also, only straight-line distance
        is supported with line geometries, but points can use available transportation
        network methods [typically drive distance or drive time].

        While already popular for site analysis, forecast modeling for a store or
        facility location, enrich provides access to a massive amount of data for any
        analysis of people and their relationship and interaction with the surrounding
        community, culture, economy and even the natural environment. Succinctly,
        enrich is how to access data for human geography analysis.

        The study_areas for enrichment can be provided in a number of forms: a Spatially
        Enabled Pandas Data Frame or an Iterable my be provided. The iterable may be
        comprised of :class:`~arcgis.geometry.Geometry` object instances or standard
        geography identifiers. While other values, such as string addresses or
        points-of-interest names may be provided, it is recommended to retrieve these
        locations in your workflow before performing enrichment.

        ============================     ====================================================================
        **Argument**                     **Description**
        ----------------------------     --------------------------------------------------------------------
        study_areas                      Required list, FeatureSet or SpatiallyEnabledDataFrame containing
                                         the input areas to be enriched.
        ----------------------------     --------------------------------------------------------------------
        enrich_variables                 Enrich variables can be specified using either a list of strings or
                                         the Pandas DataFrame returned from the 'Country.enrich_variables`
                                         property. If using a list of strings, the values are mached against
                                         the :func:`arcgis.geoenrichment.Country.enrich_variables` dataframe
                                         columns for `name`, 'enrich_name', or 'enrich_field_name'. All the
                                         values must match to one of these columns.
        ----------------------------     --------------------------------------------------------------------
        return_geometry                  Boolean indicating if the geometry needs to be returned in the
                                         output. The default is ``True``.
        ----------------------------     --------------------------------------------------------------------
        standard_geography_level         If using a list of standard geography identifiers, the geography
                                         level must be specified here. This value is the ``level_name``
                                         column retrieved in the
                                         :func:`arcgis.geoenrichment.Country.levels` property.
        ----------------------------     --------------------------------------------------------------------
        standard_geography_id_column     If providing a Pandas DataFrame as input, and the DataFrame contains
                                         a column with standard geography identifiers you desire to use for
                                         specifying the input study_areas, please provide the name of the
                                         column as a string in this parameter.
        ----------------------------     --------------------------------------------------------------------
        proximity_type                   If providing point geometries as input study_areas, you have the
                                         option to provide the method used to create the proximity around the
                                         point based on the available travel modes. These travel modes can
                                         be discovered using the ``Country.travel_modes`` property. Valid
                                         values are from the ``name`` column in this returned DataFrame.
                                         Also, in addition to the transportation network travel modes, you
                                         also have the option of using ``straight_line``, just using a
                                         straight line distance, a buffer, around the geometry. This is the
                                         default, and the only option if the geometry type is line.
        ----------------------------     --------------------------------------------------------------------
        proximity_value                  This is the scalar value as either a decimal float or integer
                                         defining the size of the proximity zone around the source geometry
                                         to be used for enrichment. For instance, if desiring a five minute
                                         drive time, this value will be ``5``.
        ----------------------------     --------------------------------------------------------------------
        proximity_metric                 This is the unit of measure defining the area to be used in defining
                                         the area surrounding geometries to use for enrichment. If interested
                                         in getting a five minute drive time, this value will be ``minutes``.
        ----------------------------     --------------------------------------------------------------------
        output_spatial_reference         The default output will be WGS84 (WKID 4326). If a different output
                                         spatial reference is desired, please provide it here as a WKID or
                                         ``arcgis.features.SpatialReference`` object instance.
        ============================     ====================================================================

        :return:
            Pandas DataFrame with enriched data.

        Here is an example of using ArcGIS Pro with Business Analyst and the United
        States data pack installed locally to enrich with a few key variables.

        .. code-block:: python

            from arcgis.gis import GIS
            from arcgis.geoenrichment import Country

            # create country object instance to use local ArcGIS Pro + Business Analyst + USA data pack
            usa = Country('usa', gis=GIS('pro'))

            # select current year key enrichment variables for analysis
            ev_df = usa.enrich_variables
            kv_df = ev_df[
                (ev_df.data_collection.str.lower().str.contains('key'))  # key data collection
                & (ev_df.alias.str.lower().str.endswith('cy'))           # current year
            ]

            # get data from ArcGIS Online to enrich as Spatially Enabled DataFrame
            itm_id = '15d227c6da8d4b7baf713709ba3693ce'  # USA federal district court polygons
            gis = GIS()  # anonymous connection to ArcGIS Online
            aoi_df = gis.content.get(itm_id).layers[0].query().sdf

            # enrich with variables selected above
            enrich_df = usa.enrich(aoi_df, enrich_variables=kv_df)

        Next, we can perform a similar workflow using ArcGIS Online instead of ArcGIS Pro by creating
        a couple of point geometries and using five-minute drive times around the locations.

        .. code-block:: python

            import os

            from arcgis.gis import GIS
            from arcgis.geoenrichment import Country
            from arcgis.geometry import Geometry
            from dotenv import find_dotenv, load_dotenv

            # load environment settings from .env file
            load_dotenv(find_dotenv())

            # create connection to ArcGIS Online organization using values saved in .env file
            gis_agol = GIS(
                url=os.getenv('ESRI_GIS_URL'),
                username=os.getenv('ESRI_GIS_USERNAME'),
                password=os.getenv('ESRI_GIS_PASSWORD')
            )

            # create a country object instance
            usa = Country('usa', gis=gis_agol)

            # get just key variables for the current year
            ev_df = usa.enrich_variables
            kv_df = ev_df[
                (ev_df.data_collection.str.lower().str.contains('key'))  # key data collection
                & (ev_df.alias.str.lower().str.endswith('cy'))           # current year
            ]

            # create a couple of point geometries on the fly for the example
            coord_lst = [
                (-122.9074835, 47.0450249),  # Bayview Grocery Store
                (-122.8749600, 47.0464031)   # Ralph's Thriftway Grocery Store
            ]
            geom_lst = [Geometry({'x': pt[0], 'y': pt[1], 'spatialReference': {'wkid': 4326}}) for pt in coord_lst]

            # enrich the geometries and get a spatially enabled dataframe
            enrich_df = usa.enrich(
                study_areas=geom_lst,
                enrich_variables=kv_df,
                proximity_type='driving_time',
                proximity_value=5,
                proxmity_metric='minutes'
            )

        Finally, we can also use standard geography identifiers to specify the study_areas as well.

        .. code-block:: python

            import os

            from arcgis.gis import GIS
            from arcgis.geoenrichment import Country
            from arcgis.geometry import Geometry
            from dotenv import find_dotenv, load_dotenv

            # load environment settings from .env file
            load_dotenv(find_dotenv())

            # create connection to ArcGIS Online organization using values saved in .env file
            gis_agol = GIS(
                url=os.getenv('ESRI_GIS_URL'),
                username=os.getenv('ESRI_GIS_USERNAME'),
                password=os.getenv('ESRI_GIS_PASSWORD')
            )

            # create a country object instance
            usa = Country('usa', gis=gis_agol)

            # get just key variables for the current year
            ev_df = usa.enrich_variables
            kv_df = ev_df[
                (ev_df.data_collection.str.lower().str.contains('key'))  # key data collection
                & (ev_df.alias.str.lower().str.endswith('cy'))           # current year
            ]

            # the block group ids for Olympia, WA
            id_lst = ['530670101001', '530670101002', '530670101003', '530670101004', '530670102001', '530670102002',
                      '530670102003', '530670103001', '530670103002', '530670103003', '530670103004', '530670104001',
                      '530670104002', '530670104003', '530670105101', '530670105201', '530670105202', '530670105203',
                      '530670105204', '530670106001', '530670106002', '530670106003', '530670106004', '530670106005',
                      '530670107001', '530670107002', '530670107003', '530670108001', '530670108002', '530670109101',
                      '530670109102', '530670109103', '530670110001', '530670111002', '530670112001', '530670113001',
                      '530670116211', '530670117101', '530670117102', '530670117103', '530670120002', '530670122121',
                      '530670122122', '530670122124', '530670111001', '530670121004']

            # enrich the geometries and get a spatially enabled dataframe
            enrich_df = usa.enrich(
                study_areas=geom_lst,
                enrich_variables=kv_df,
                standard_geography_level='block_groups'
            )

        """
        # pull out named areas if present
        if isinstance(study_areas, Iterable) and not isinstance(
            study_areas, pd.DataFrame
        ):
            first_geo = study_areas[0]
            if isinstance(first_geo, NamedArea):
                study_areas = [na._areaid for na in study_areas]
                standard_geography_level = first_geo._currlvl

        # if data collections passed in kwargs, pull enrich variables out
        if "data_collections" in kwargs.keys():
            enrich_variables = _preproces_data_colletions_and_analysis_variables(
                self, kwargs["data_collections"], enrich_variables
            )

        # invoke enrich on the business analyst object
        enrich_res = self._ba_cntry.enrich(
            study_areas,
            enrich_variables,
            return_geometry,
            standard_geography_level,
            standard_geography_id_column,
            proximity_type,
            proximity_value,
            proximity_metric,
            output_spatial_reference,
            **kwargs,
        )
        return enrich_res

    @_lazy_property
    @local_vs_gis
    def subgeographies(self):
        """
        Returns the named geographical places in this country, as NamedArea objects. Each named area has attributes for
        the supported subgeography levels within it, and the value of those attributes are dictionaries containing the
        named places within that level of geography. This allows for interactive selection of places using intellisense
        and a notation such as the following:

        .. code-block:: python

            # Usage Example 1

            usa = Country.get('USA')
            usa.subgeographies.states['California'].counties['San_Bernardino_County']

        .. code-block:: python

                # Usage Example 2

                india.named_places.states['Bihar'].districts['Aurangabad'].subdistricts['Barun']

        .. note::

            Currently this is only supported when using a Web GIS (ArcGIS Online
            or ArcGIS Enterprise as the ``gis`` source.
        """
        pass

    def _subgeographies_gis(self):
        """GIS implementation of subgeographies."""
        return NamedArea(self)

    @local_vs_gis
    def search(self, query, layers=["*"]):
        """
        Searches this country for places that have the specified query string in their name.

        Returns a list of named areas matching the specified query

        ================  ========================================================
        **Argument**      **Description**
        ----------------  --------------------------------------------------------
        query             Required string. The query string to search for places
                          within this country.
        ----------------  --------------------------------------------------------
        levels            Optional list of layer ids. Layer ids for a country
                          can be queried using Country.levels properties.
        ================  ========================================================

        :return:
            A list of named areas that match the query string

        .. note::

            Currently this is only supported when using a Web GIS (ArcGIS Online
            or ArcGIS Enterprise as the ``gis`` source.
        """

    def _search_gis(self, query, layers=["*"]):
        """GIS search implementation."""
        df = standard_geography_query(
            source_country=self.properties.iso2,
            geoquery=query,
            layers=layers,
            return_geometry=True,
            as_featureset=False,
            gis=self._gis,
        )

        places = []
        for index, row in df.iterrows():
            plc = dict(row)
            place = NamedArea(
                country=self,
                name=plc["AreaName"],
                level=plc["DataLayerID"],
                areaid=plc["AreaID"],
                geometry=plc["SHAPE"],
            )
            places.append(place)

        return places

    @_lazy_property
    @local_vs_gis
    def reports(self):
        """
        Returns the available reports for this country as a Pandas dataframe.

        .. note::

            Currently this is only supported when using a Web GIS (ArcGIS Online
            or ArcGIS Enterprise as the ``gis`` source.
        """
        pass

    def _reports_gis(self):
        """GIS implementation of reports."""
        rdf = _find_report(self.properties.iso2)
        df = pd.json_normalize(rdf)
        df = df[
            ["reportID", "metadata.title", "metadata.categories", "formats"]
        ].rename(
            columns={
                "reportID": "id",
                "metadata.title": "title",
                "metadata.categories": "categories",
            }
        )
        return df

    @_lazy_property
    def travel_modes(self):
        """
        DataFrame of available travel modes for the country. This is determined by what
        transportation networks are available for the given country.

        For instance, running the following code in Jupyter will return the table below.

        .. code-block:: python

            from arcgis.gis import GIS
            from arcgis.geoenrichment import Country

            usa = Country('usa', gis=GIS('pro'))

            usa.travel_modes

        .. list-table::
            :widths: 1 17 17 30 10 15 8 15 10
            :header-rows: 1

            * -
              - name
              - alias
              - description
              - type
              - impedance
              - impedance_category
              - time_attribute_name
              - distance_attribute_name
            * - 0
              - driving_time
              - Driving Time
              - Models the movement of cars...
              - AUTOMOBILE
              - TravelTime
              - temporal
              - TravelTime
              - Kilometers
            * - 1
              - driving_distance
              - Driving Distance
              - Models the movement of cars...
              - AUTOMOBILE
              - Kilometers
              - distance
              - TravelTime
              - Kilometers
            * - 2
              - trucking_time
              - Trucking Time
              - Models basic truck travel b...
              - TRUCK
              - TruckTravelTime
              - temporal
              - TruckTravelTime
              - Kilometers
            * - 3
              - trucking_distance
              - Trucking Distance
              - Models basic truck travel b...
              - TRUCK
              - Kilometers
              - distance
              - TruckTravelTime
              - Kilometers
            * - 4
              - walking_time
              - Walking Time
              - Follows paths and roads tha...
              - WALK
              - WalkTime
              - temporal
              - WalkTime
              - Kilometers

        .. note::

            The values in the ``name`` column are what is required for the
            ``proximity_type`` parameter for the
            :func:`~arcgis.geoenrichment.Country.enrich` method.

        """
        return self._ba_cntry.travel_modes


def get_countries(gis: Optional[GIS] = None, as_df: bool = True):
    """
    Retrieve available countries based on the GIS source being used.

    ==================     ====================================================================
    **Argument**           **Description**
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` instance. This specifies what GIS
                           country sources are available based on the Web GIS source, whether
                           it be `ArcGIS Online`, `ArcGIS Enterprise`, or `ArcGIS Pro with the
                           Business Analyst extension and at least one country data pack`. If
                           not specified, it tries to use an active GIS already created in the
                           Python session. If an active GIS is not available, it then tries to
                           use local resourcea, ArcGIS Pro with Business and at least one
                           country dataset installed locally. Finally, if neither of these
                           sources are available, a :class:`~arcgis.gis.GIS` object must be
                           explicitly provided.
    ------------------     --------------------------------------------------------------------
    as_df                  Optional boolean, specifying if a Pandas DataFrame output is
                           desired. If ``True``, (the default) a Pandas DataFrame of available
                           countries is returned. If ``False`` , a list of
                           :class:`~arcgis.geoenrichment.Country` objects is returned.
    ==================     ====================================================================

    :return:
        Available countries as a list of :class:`~arcgis.geoenrichment.Country` objects, or a
        Pandas DataFrame of available countries.
    """
    # preprocess the gis object to determine if a local (ArcGIS Pro) gis source
    gis = _check_gis_source(gis)

    # get the dataframe of available countries
    out_res = _business_analyst.BusinessAnalyst(gis).countries

    # if a dataframe is not desired, use the ISO3 codes to crate a list of Countries from the ISO3 codes
    if as_df is False:
        if "vintage" in out_res.columns:
            out_res = [
                Country(cntry[1][0], gis=gis, year=cntry[1][1])
                for cntry in out_res[["iso3", "vintage"]].iterrows()
            ]
        else:
            out_res = [
                Country(cntry[1], gis=gis) for cntry in out_res["iso3"].iteritems()
            ]

    return out_res


@_call_method_by_source
def create_report(
    study_areas,
    report=None,
    export_format="pdf",
    report_fields=None,
    options=None,
    return_type=None,
    use_data=None,
    in_sr=4326,
    out_name=None,
    out_folder=None,
    gis=None,
):
    """
    The Create Report method allows you to create many types of high quality reports for a
    variety of use cases describing the input area. If a point is used as a study area, the
    service will create a 1-mile ring buffer around the point to collect and append enrichment
    data. Optionally, you can create a buffer ring or drive-time service area around points of
    interest to generate PDF or Excel reports containing relevant information for the area on
    demographics, consumer spending, tapestry market, business or market potential.

    Report options are available and can be used to describe and gain a better understanding
    about the market, customers / clients and competition associated with an area of interest.


    ==================     ====================================================================
    **Argument**           **Description**
    ------------------     --------------------------------------------------------------------
    study_areas            required list. Required parameter: Study areas may be defined by
                           input points, polygons, administrative boundaries or addresses.
    ------------------     --------------------------------------------------------------------
    report                 optional string. identify the id of the report. This may be one of
                           the many default reports available along with our demographic data
                           collections or a customized report. Custom report templates are
                           stored in an ArcGIS Online organization as a Report Template item.
                           The organization URL and a valid ArcGIS Online authentication token
                           is required for security purposes to access these templates. If no
                           report is specified, the default report is census profile for United
                           States and a general demographic summary report for most countries.
    ------------------     --------------------------------------------------------------------
    export_format          Optional parameter to specify the format of the generated report.
                           Supported formats include PDF and XLSX.
    ------------------     --------------------------------------------------------------------
    report_fields          Optional parameter specifies additional choices to customize
                           reports. Below is an example of the position on the report header
                           for each field.
    ------------------     --------------------------------------------------------------------
    options                Optional parameter to specify the properties for the study area
                           buffer. For a full list of valid buffer properties values and
                           further examples review the Input XY Locations' options parameter.

                           By default a 1 mile radius buffer will be applied to point(s) and
                           address locations to define a study area.
    ------------------     --------------------------------------------------------------------
    return_type            Optional parameter used for storing an output report item to Portal
                           for ArcGIS instead of returning a report to a customer via binary
                           stream. The attributes are used by Portal to determine where and how
                           an item is stored. Parameter attributes include: user, folder,
                           title, item_properties, URL, token, and referrer.
                           Example

                           Creating a new output in a Portal for ArcGIS Instance:

                           return_type = {'user' : 'testUser',
                                          'folder' : 'FolderName',
                                          'title' : 'Report Title',
                                          'item_properties' : '<properties>',
                                          'url' : 'https://hostname.domain.com/webadaptor',
                                          'token' : 'token', 'referrer' : 'referrer'}
    ------------------     --------------------------------------------------------------------
    use_data               Optional dictionary. This parameter explicitly specify the country
                           or dataset to query. When all input features specified in the
                           study_areas parameter describe locations or areas that lie in the
                           same country or dataset, this parameter can be specified to provide
                           an additional 'performance hint' to the service.

                           By default, the service will automatically determine the country or
                           dataset that is associated with each location or area submitted in
                           the study_areas parameter. Specifying a specific dataset or country
                           through this parameter will potentially improve response time.

                           By default, the data apportionment method is determined by the size
                           of the study area. Small study areas use block apportionment for
                           higher accuracy whereas large study areas (100 miles or more) will
                           use a cascading centroid apportionment method to maintain
                           performance. This default behavior can be overridden by using the
                           detailed_aggregation parameter.
    ------------------     --------------------------------------------------------------------
    in_sr                  Optional parameter to define the input geometries in the study_areas
                           parameter in a specified spatial reference system.
                           When input points are defined in the study_areas parameter, this
                           optional parameter can be specified to explicitly indicate the
                           spatial reference system of the point features. The parameter value
                           can be specified as the well-known ID describing the projected
                           coordinate system or geographic coordinate system.
                           The default is 4326
    ------------------     --------------------------------------------------------------------
    out_name               Optional string.  Name of the output file [ending in .pdf or .xlsx)
    ------------------     --------------------------------------------------------------------
    out_folder             Optional string. Name of the save folder
    ==================     ====================================================================
    """
    pass


def _create_report_gis(
    study_areas,
    report=None,
    export_format="pdf",
    report_fields=None,
    options=None,
    return_type=None,
    use_data=None,
    in_sr=4326,
    out_name=None,
    out_folder=None,
    gis=None,
):
    """GIS implementation of create report."""
    if gis is None:
        gis = env.active_gis

    areas = []
    for area in study_areas:
        area_dict = area
        if isinstance(
            area, str
        ):  # street address - {"address":{"text":"380 New York St Redlands CA 92373"}}
            area_dict = {"address": {"text": area}}
        elif isinstance(area, dict):  # pass through - user knows what they're sending
            pass
        elif isinstance(area, Geometry):  # geometry, polygons, points
            area_dict = {"geometry": dict(area)}
        elif isinstance(area, BufferStudyArea):

            # namedtuple('BufferStudyArea', 'area radii units overlap travel_mode')
            g = area.area
            if isinstance(g, str):
                area_dict = {"address": {"text": g}}
            elif isinstance(g, dict):
                area_dict = g
            elif isinstance(g, Geometry):  # geometry, polygons, points
                area_dict = {"geometry": dict(g)}
            else:
                raise ValueError(
                    "BufferStudyArea is only supported for Point geometry and addresses"
                )

            area_type = "RingBuffer"
            if area.travel_mode is None:
                if not area.overlap:
                    area_type = "RingBufferBands"
            else:
                area_type = "NetworkServiceArea"

            area_dict["areaType"] = area_type
            area_dict["bufferUnits"] = area.units
            area_dict["bufferRadii"] = area.radii
            if area.travel_mode is not None:
                area_dict["travel_mode"] = area.travel_mode

        elif isinstance(area, NamedArea):  # named area
            area_dict = area.__studyarea__
        elif isinstance(area, list):  # list of named areas, (union)
            first_area = area[0]
            ids = []
            if isinstance(first_area, NamedArea):
                for namedarea in area:
                    a = namedarea.__studyarea__
                    if (
                        a["layer"] != first_area["layer"]
                        or a["sourceCountry"] != first_area["sourceCountry"]
                    ):
                        raise ValueError(
                            "All NamedAreas in the list must have the same source country and level"
                        )
                    ids.append(a["ids"])
                area_dict = {
                    "sourceCountry": first_area["sourceCountry"],
                    "layer": first_area["layer"],
                    "ids": [ids.join(",")],
                }
            else:
                raise ValueError("Lists members must be NamedArea instances")
        else:
            raise ValueError(
                "Don't know how to handle study areas of type " + str(type(area))
            )

        areas.append(area_dict)
    ge = _GeoEnrichment(gis=gis)
    return ge.create_report(
        study_areas=areas,
        report=report,
        export_format=export_format,
        report_fields=report_fields,
        options=options,
        return_type=return_type,
        use_data=use_data,
        in_sr=in_sr,
        out_folder=out_folder,
        out_name=out_name,
    )


# ----------------------------------------------------------------------
def _data_collections(
    country=None,
    collection_name=None,
    variables=None,
    out_fields="*",
    hide_nulls=True,
    gis=None,
    as_dict=True,
):
    """
    The GeoEnrichment class uses the concept of a data collection to define the data
    attributes returned by the enrichment service. Each data collection has a unique name
    that acts as an ID that is passed in the data_collections parameter of the GeoEnrichment
    service.

    Some data collections (such as default) can be used in all supported countries. Other data
    collections may only be available in one or a collection of countries. Data collections may
    only be available in a subset of countries because of differences in the demographic data
    that is available for each country. A list of data collections for all available countries
    can be generated with the data collection discover method seen below.
    Return a list of data collections that can be run for any country.

    ==================     ====================================================================
    **Argument**           **Description**
    ------------------     --------------------------------------------------------------------
    country                optional string. lets the user supply and optional name of a country
                           in order to get information about the data collections in that given
                           country.
    ------------------     --------------------------------------------------------------------
    dataset                Optional string. Name of the data collection to examine.
    ------------------     --------------------------------------------------------------------
    variables              Optional string/list. This parameter to specifies a list of field
                           names that include variables for the derivative statistics.
    ------------------     --------------------------------------------------------------------
    out_fields             Optional string. This parameter is a string of comma seperate field
                           names.
    ------------------     --------------------------------------------------------------------
    hide_nulls             Optional boolean. parameter to return only values that are not NULL
                           in the output response. Adding the optional suppress_nulls parameter
                           to any data collections discovery method will reduce the size of the
                           output that is returned.
    ------------------     --------------------------------------------------------------------
    gis                    Optional GIS.  If None, the GIS object will be used from the
                           arcgis.env.active_gis.  This GIS object must be authenticated and
                           have the ability to consume credits
    ------------------     --------------------------------------------------------------------
    as_dict                Opional boolean. If True, the result comes back as a python
                           dictionary, else the value will returns as a Python DataFrame.
    ==================     ====================================================================

    :return: dictionary, describing the requested return data.
    """
    if gis is None:
        gis = env.active_gis
    ge = _GeoEnrichment(gis=gis)

    return ge.data_collections(
        country=country,
        collection_name=collection_name,
        variables=variables,
        out_fields=out_fields,
        hide_nulls=hide_nulls,
        as_dict=as_dict,
    )


@_call_method_by_source
def service_limits(gis=None):
    """
    Returns a Pandas' DataFrame describing limitations for each input parameter.

    :return: Pandas DataFrame
    """
    pass


def _service_limits_gis(gis=None):
    """Local implementation of service limits."""
    if gis is None:
        gis = env.active_gis
    ge = _GeoEnrichment(gis=gis)
    return ge.limits


def enrich(
    study_areas,
    data_collections=None,
    analysis_variables=None,
    comparison_levels=None,
    add_derivative_variables=None,
    intersecting_geographies=None,
    return_geometry=True,
    gis=None,
    proximity_type=None,
    proximity_value=None,
    proximity_metric=None,
):
    """
    Enrich provides access to a massive dataset describing exactly who people are
    in a geographic location. The most common way to delineate study_areas for
    enrichment is using polygons delineated areas, although points and lines can
    be used as well.

    When points or lines are provided, an area surrounding the geometries is used
    for enrichment. This area can be defined using additional parameters, but by
    default is one kilometer around the geometry. Also, only straight-line distance
    is supported with line geometries, but points can use available transportation
    network methods – typically drive distance or drive time.

    While already popular for site analysis, forecast modeling for a store or
    facility location, enrich provides access to a massive amount of data for any
    analysis of people and their relationship and interaction with the surrounding
    community, culture, economy and even the natural environment. Succinctly,
    enrich is how to access data for human geography analysis.

    =========================     ====================================================================
    **Argument**                  **Description**
    -------------------------     --------------------------------------------------------------------
    study_areas                   Required list, FeatureSet or SpatiallyEnabledDataFrame containing
                                  the input areas to be enriched.

                                  study_areas can be a SpatiallyEnabledDataFrame, FeatureSet or a
                                  lists of the following types:
                                  * addresses, points of interest, place names or other
                                  supported locations as strings.
                                  * dicts such as [{"address":{"Address":"380 New York St.",
                                  "Admin1":"Redlands","Admin2":"CA","Postal":"92373",
                                  "CountryCode":"USA"}}] for multiple field addresses
                                  * arcgis.gis.Geometry instances
                                  * BufferStudyArea instances. By default, one-mile ring
                                  buffers are created around the points to collect and append
                                  enrichment data. You can use BufferStudyArea to change the ring
                                  buffer size or create drive-time service areas around the points.
                                  * NamedArea instances to support standard geography. They are
                                  obtained using Country.subgeographies()/search(). When
                                  the NamedArea instances should be combined together (union), a list
                                  of such NamedArea instances should constitute a study area in the
                                  list of requested study areas.
    -------------------------     --------------------------------------------------------------------
    data_collections              Optional list. A Data Collection is a preassembled list of
                                  attributes that will be used to enrich the input features.
                                  Enrichment attributes can describe various types of information such
                                  as demographic characteristics and geographic context of the
                                  locations or areas submitted as input features in study_areas.
    -------------------------     --------------------------------------------------------------------
    analysis_variables            Optional list. A Data Collection is a preassembled list of
                                  attributes that will be used to enrich the input features. With the
                                  analysis_variables parameter you can return a subset of variables
                                  enrichment attributes can describe various types of information such
                                  as demographic characteristics and geographic context of the
                                  locations or areas submitted as input features in study_areas.
    -------------------------     --------------------------------------------------------------------
    add_derivative_variables      Optional list. This parameter is used to specify an array of string
                                  values that describe what derivative variables to include in the
                                  output. The list of accepted values includes:
                                  ['percent','index','average','all','*']
    -------------------------     --------------------------------------------------------------------
    comparison_levels             Optional list of layer IDs for which the intersecting
                                  study_areas should be geoenriched.
    -------------------------     --------------------------------------------------------------------
    intersecting_geographies      Optional parameter to explicitly define the geographic layers used
                                  to provide geographic context during the enrichment process. For
                                  example, you can use this optional parameter to return the U.S.
                                  county and ZIP Code that each input study area intersects.
                                  You can intersect input features defined in the study_areas
                                  parameter with standard geography layers that are provided by the
                                  GeoEnrichment class for each country. You can also intersect
                                  features from a publicly available feature service.
    -------------------------     --------------------------------------------------------------------
    return_geometry               Optional boolean. A parameter to request the output geometries in
                                  the response.
    -------------------------     --------------------------------------------------------------------
    gis                           Optional GIS.  If None, the GIS object will be used from the
                                  arcgis.env.active_gis.  This GIS object must be authenticated and
                                  have the ability to consume credits
    -------------------------     --------------------------------------------------------------------
    proximity_type                If the input study_areas are points, retrieving enriched
                                  variables requires delineating a zone around each point to use
                                  for apportioning demographic factors to each input geography.
                                  Default is ``straight_line``, and if the input geometry is lines,
                                  ``straight_line`` is the only valid input.
    -------------------------     --------------------------------------------------------------------
    proximity_value:              If the input study_areas are points or lines, this is the value used
                                  to create a zone around the points for apportioning demographic
                                  factors. For instance, if specifying five miles, this parameter
                                  value will be ``5``. Default is ``1``.
    -------------------------     --------------------------------------------------------------------
    proximity_metric:             If the input study_areas are point or lines, this is the metric
                                  defining the proximity value. For instance, if specifying one
                                  kilometer, this value will be ``kilometers``. Default is
                                  ``kilometers``.
    =========================     ====================================================================

    Refer to https://developers.arcgis.com/rest/geoenrichment/api-reference/street-address-locations.htm for
    the format of intersection_geographies parameter.

    :return: Spatial DataFrame or Panda's DataFrame with the requested variables for the study areas.
    """
    # handle the caveat of using a GIS('Pro') input
    gis = _check_gis_source(gis)

    # create the enrich source
    enrich_src = _business_analyst.BusinessAnalyst(gis)

    # pull out named area properties if present and set to use country instead of just BA global
    standard_geography_level = None

    if isinstance(study_areas, Iterable) and not isinstance(study_areas, pd.DataFrame):
        first_geo = study_areas[0]

        if isinstance(first_geo, NamedArea):
            study_areas = [na._areaid for na in study_areas]
            standard_geography_level = first_geo._currlvl
            enrich_src = first_geo._country

    # check if data collections used as input parameter against available data collections
    if data_collections is not None:
        avail_data_coll = enrich_src.enrich_variables.data_collection.unique()
        unavail_data_coll = [dc for dc in data_collections if dc not in avail_data_coll]
        assert len(unavail_data_coll) == 0, (
            "One or more of the data collections you requested is not available. The "
            'only data\ncollection available globally is "KeyGlobalFacts". For '
            "working with data specific to a country, you\ncan discover available "
            "data collections using "
            "Country.enrich_variables.data_collection.unique(),\nand enrich using "
            "the Country.enrich method."
        )

    # get all possible requested enrich variables
    src = enrich_src._ba_cntry if isinstance(enrich_src, Country) else enrich_src
    enrich_vars = _preproces_data_colletions_and_analysis_variables(
        src, data_collections, analysis_variables
    )

    # invoke enrich on the business analyst object
    enrich_res = enrich_src.enrich(
        geographies=study_areas,
        enrich_variables=enrich_vars,
        proximity_type=proximity_type,
        proximity_value=proximity_value,
        proximity_metric=proximity_metric,
        standard_geography_level=standard_geography_level,
        return_geometry=return_geometry,
    )

    return enrich_res


def _preproces_data_colletions_and_analysis_variables(
    src: Union[_business_analyst.BusinessAnalyst, _business_analyst.Country],
    data_cols: list,
    enrich_vars: list,
    **kwargs,
) -> pd.DataFrame:
    """helper function to"""
    # since supporting data collections, get the variables
    if data_cols is not None:
        data_cols = data_cols if isinstance(data_cols, list) else [data_cols]
        dc_vars = src.enrich_variables[
            src.enrich_variables["data_collection"].isin(data_cols)
        ]
    else:
        dc_vars = None

    # if variables provided, prep as well
    if enrich_vars is not None and not isinstance(enrich_vars, pd.DataFrame):
        av_vars = src.get_enrich_variables_from_iterable(enrich_vars, **kwargs)
    else:
        av_vars = None

    # if variables coming from both sources, combine
    if av_vars is not None and dc_vars is not None:
        enrich_vars = pd.concat([av_vars, dc_vars])
    elif av_vars is not None:
        enrich_vars = av_vars
    else:
        enrich_vars = dc_vars

    return enrich_vars


# ----------------------------------------------------------------------
def _find_report(country, gis=None):
    """
    Returns a list of reports by a country code

    ==================     ====================================================================
    **Argument**           **Description**
    ------------------     --------------------------------------------------------------------
    country                optional string. lets the user supply and optional name of a country
                           in order to get information about the data collections in that given
                           country. This should be a two country code name.
                           Example: United States as US
    ------------------     --------------------------------------------------------------------
    gis                    Optional GIS.  If None, the GIS object will be used from the
                           arcgis.env.active_gis.  This GIS object must be authenticated and
                           have the ability to consume credits
    ==================     ====================================================================

    :return: Panda's DataFrame
    """
    if gis is None:
        gis = env.active_gis
    ge = _GeoEnrichment(gis=gis)
    return ge.find_report(country=country)


# ----------------------------------------------------------------------
@_call_method_by_source
def standard_geography_query(
    source_country=None,
    country_dataset=None,
    layers=None,
    ids=None,
    geoquery=None,
    return_sub_geography=False,
    sub_geography_layer=None,
    sub_geography_query=None,
    out_sr=4326,
    return_geometry=False,
    return_centroids=False,
    generalization_level=0,
    use_fuzzy_search=False,
    feature_limit=1000,
    as_featureset=False,
    gis=None,
):
    """
    This method allows you to search and query standard geography areas so that they can be used to
    obtain facts about the location using the enrich() method or create reports about.

    GeoEnrichment uses the concept of a study area to define the location of the point
    or area that you want to enrich with additional information. Locations can also be passed as
    one or many named statistical areas. This form of a study area lets you define an area by
    the ID of a standard geographic statistical feature, such as a census or postal area. For
    example, to obtain enrichment information for a U.S. state, county or ZIP Code or a Canadian
    province or postal code, the Standard Geography Query helper method allows you to search and
    query standard geography areas so that they can be used in the GeoEnrichment method to
    obtain facts about the location.
    The most common workflow for this service is to find a FIPS (standard geography ID) for a
    geographic name. For example, you can use this service to find the FIPS for the county of
    San Diego which is 06073. You can then use this FIPS ID within the GeoEnrichment class study
    area definition to get geometry and optional demographic data for the county. This study
    area definition is passed as a parameter to the GeoEnrichment class to return data defined
    in the enrichment pack and optionally return geometry for the feature.

    ======================     ====================================================================
    **Argument**               **Description**
    ----------------------     --------------------------------------------------------------------
    source_country             Optional string. to specify the source country for the search. Use
                               this parameter to limit the search and query of standard geographic
                               features to one country. This parameter supports both the two-digit
                               and three-digit country codes illustrated in the coverage table.
    ----------------------     --------------------------------------------------------------------
    country_dataset            Optional string. parameter to specify a specific dataset within a
                               defined country.
    ----------------------     --------------------------------------------------------------------
    layers                     Optional list/string. Parameter specifies which standard geography
                               layers are being queried or searched. If this parameter is not
                               provided, all layers within the defined country will be queried.
    ----------------------     --------------------------------------------------------------------
    ids                        Optional parameter to specify which IDs for the standard geography
                               layers are being queried or searched. You can use this parameter to
                               return attributes and/or geometry for standard geographic areas for
                               administrative areas where you already know the ID, for example, if
                               you know the Federal Information Processing Standard (FIPS) Codes for
                               a U.S. state or county; or, in Canada, to return the geometry and
                               attributes for a Forward Sortation Area (FSA).
                               Example:
                               Return the state of California where the layers parameter is set to
                               layers=['US.States']
                               then set ids=["06"]
    ----------------------     --------------------------------------------------------------------
    geoquery                   Optional string/list. This parameter specifies the text to query
                               and search the standard geography layers specified. You can use this
                               parameter to query and find standard geography features that meet an
                               input term, for example, for a list of all the U.S. counties that
                               contain the word "orange". The geoquery parameter can be a string
                               that contains one or more words.
    ----------------------     --------------------------------------------------------------------
    return_sub_geography       Optional boolean. Use this optional parameter to return all the
                               subgeographic areas that are within a parent geography.
                               For example, you could return all the U.S. counties for a given
                               U.S. state or you could return all the Canadian postal areas
                               (FSAs) within a Census Metropolitan Area (city).
                               When this parameter is set to true, the output features will be
                               defined in the sub_geography_layer. The output geometries will be
                               in the spatial reference system defined by out_sr.
    ----------------------     --------------------------------------------------------------------
    sub_geography_layer        Optional string/list. Use this optional parameter to return all the
                               subgeographic areas that are within a parent geography. For example,
                               you could return all the U.S. counties within a given U.S. state or
                               you could return all the Canadian postal areas (FSAs) within a
                               Census Metropolitan Areas (city).
                               When this parameter is set to true, the output features will be
                               defined in the sub_geography_layer. The output geometries will be
                               in the spatial reference system defined by out_sr.
    ----------------------     --------------------------------------------------------------------
    sub_geography_query        Optional string.User this parameter to filter the results of the
                               subgeography features that are returned by a search term.
                               You can use this parameter to query and find subgeography
                               features that meet an input term. This parameter is used to
                               filter the list of subgeography features that are within a
                               parent geography. For example, you may want a list of all the
                               ZIP Codes that are within "San Diego County" and filter the
                               results so that only ZIP Codes that start with "921" are
                               included in the output response. The subgeography query is a
                               string that contains one or more words.
    ----------------------     --------------------------------------------------------------------
    out_sr                     Optional integer Use this parameter to request the output geometries
                               in a specified spatial reference system.
    ----------------------     --------------------------------------------------------------------
    return_geometry            Optional boolean. Use this parameter to request the output
                               geometries in the response.  The return type will become a Spatial
                               DataFrame instead of a Panda's DataFrame.
    ----------------------     --------------------------------------------------------------------
    return_centroids           Optional Boolean.  Use this parameter to request the output geometry
                               to return the center point for each feature.
    ----------------------     --------------------------------------------------------------------
    generalization_level       Optional integer that specifies the level of generalization or
                               detail in the area representations of the administrative boundary or
                               standard geographic data layers.
                               Values must be whole integers from 0 through 6, where 0 is most
                               detailed and 6 is most generalized.
    ----------------------     --------------------------------------------------------------------
    use_fuzzy_search           Optional Boolean parameter to define if text provided in the
                               geoquery parameter should utilize fuzzy search logic. Fuzzy searches
                               are based on the Levenshtein Distance or Edit Distance algorithm.
    ----------------------     --------------------------------------------------------------------
    feature_limit              Optional integer value where you can limit the number of features
                               that are returned from the geoquery.
    ----------------------     --------------------------------------------------------------------
    as_featureset              Optional boolean.  The default is False. If True, the result will be
                               a arcgis.features.FeatureSet object instead of a SpatailDataFrame or
                               Pandas' DataFrame.
    ----------------------     --------------------------------------------------------------------
    gis                        Optional GIS.  If None, the GIS object will be used from the
                               arcgis.env.active_gis.  This GIS object must be authenticated and
                               have the ability to consume credits
    ======================     ====================================================================

    :return: Spatial or Pandas Dataframe on success, FeatureSet, or dictionary on failure.

    """
    pass


def _standard_geography_query_gis(
    source_country=None,
    country_dataset=None,
    layers=None,
    ids=None,
    geoquery=None,
    return_sub_geography=False,
    sub_geography_layer=None,
    sub_geography_query=None,
    out_sr=4326,
    return_geometry=False,
    return_centroids=False,
    generalization_level=0,
    use_fuzzy_search=False,
    feature_limit=1000,
    as_featureset=False,
    gis=None,
):
    """GIS implementation of standard_geography_query."""
    if gis is None:
        gis = env.active_gis
    ge = _GeoEnrichment(gis=gis)

    return ge.standard_geography_query(
        source_country=source_country,
        country_dataset=country_dataset,
        layers=layers,
        ids=ids,
        geoquery=geoquery,
        return_sub_geography=return_sub_geography,
        sub_geography_layer=sub_geography_layer,
        sub_geography_query=sub_geography_query,
        out_sr=out_sr,
        return_geometry=return_geometry,
        return_centroids=return_centroids,
        generalization_level=generalization_level,
        use_fuzzy_search=use_fuzzy_search,
        feature_limit=feature_limit,
        as_featureset=as_featureset,
    )
