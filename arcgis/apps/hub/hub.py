from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from arcgis._impl.common._mixins import PropertyMap
from arcgis.geocoding import geocode
from arcgis.apps.hub.sites import SiteManager, Site, PageManager
from datetime import datetime
from collections import OrderedDict
import json


def _lazy_property(fn):
    """Decorator that makes a property lazy-evaluated."""
    # http://stevenloria.com/lazy-evaluated-properties-in-python/
    attr_name = "_lazy_" + fn.__name__

    @property
    def _lazy_property(self):
        if not hasattr(self, attr_name):
            setattr(self, attr_name, fn(self))
        return getattr(self, attr_name)

    return _lazy_property


class Hub(object):
    """
    Entry point into the Hub module. Lets you access an individual hub and its components.


    ================    ===============================================================
    **Argument**        **Description**
    ----------------    ---------------------------------------------------------------
    url                 Required string. If no URL is provided by user while connecting
                        to the GIS, then the URL will be ArcGIS Online.
    ----------------    ---------------------------------------------------------------
    username            Optional string as entered while connecting to GIS. The login user name
                        (case-sensitive).
    ----------------    ---------------------------------------------------------------
    password            Optional string as entered while connecting to GIS. If a username is
                        provided, a password is expected.  This is case-sensitive. If the password
                        is not provided, the user is prompted in the interactive dialog.
    ================    ===============================================================

    """

    def __init__(self, gis):
        self.gis = gis
        try:
            self._gis_id = self.gis.properties.id
        except AttributeError:
            self._gis_id = None

    @property
    def _hub_enabled(self):
        """
        Returns True if Hub Premium is enabled on this org
        """
        try:
            self.gis.properties.portalProperties["hub"]["enabled"]
            return True
        except:
            return False

    @property
    def enterprise_org_id(self):
        """
        Returns the AGOL org id of the Enterprise Organization associated with this Premium Hub.
        """

        if self._hub_enabled:
            try:
                _e_org_id = (
                    self.gis.properties.portalProperties.hub.settings.enterpriseOrg.orgId
                )
                return _e_org_id
            except AttributeError:
                try:
                    if (
                        self.gis.properties.subscriptionInfo.companionOrganizations.type
                        == "Enterprise"
                    ):
                        return "Enterprise org id is not available"
                except:
                    return self._gis_id
        else:
            raise Exception("Hub does not exist or is inaccessible.")

    @property
    def community_org_id(self):
        """
        Returns the AGOL org id of the Community Organization associated with this Premium Hub.
        """
        if self._hub_enabled:
            try:
                _c_org_id = (
                    self.gis.properties.portalProperties.hub.settings.communityOrg.orgId
                )
                return _c_org_id
            except AttributeError:
                try:
                    if (
                        self.gis.properties.subscriptionInfo.companionOrganizations.type
                        == "Community"
                    ):
                        return "Community org id is not available"
                except:
                    return self._gis_id
        else:
            raise Exception("Hub does not exist or is inaccessible.")

    @property
    def enterprise_org_url(self):
        """
        Returns the AGOL org url of the Enterprise Organization associated with this Premium Hub.
        """
        if self._hub_enabled:
            try:
                self.gis.properties.portalProperties.hub.settings.enterpriseOrg
                try:
                    _url = self.gis.properties.publicSubscriptionInfo.companionOrganizations[
                        0
                    ][
                        "organizationUrl"
                    ]
                except:
                    _url = self.gis.properties.subscriptionInfo.companionOrganizations[
                        0
                    ]["organizationUrl"]
                return "https://" + _url
            except AttributeError:
                return self.gis.url
        else:
            raise Exception("Hub does not exist or is inaccessible.")

    @property
    def community_org_url(self):
        """
        Returns the AGOL org id of the Community Organization associated with this Premium Hub.
        """
        if self._hub_enabled:
            try:
                self.gis.properties.portalProperties.hub.settings.communityOrg
                try:
                    _url = self.gis.properties.publicSubscriptionInfo.companionOrganizations[
                        0
                    ][
                        "organizationUrl"
                    ]
                except:
                    _url = self.gis.properties.subscriptionInfo.companionOrganizations[
                        0
                    ]["organizationUrl"]
                return "https://" + _url
            except AttributeError:
                return self.gis.url
        else:
            raise Exception("Hub does not exist or is inaccessible.")

    @_lazy_property
    def initiatives(self):
        """
        The resource manager for Hub initiatives. See :class:`~arcgis.apps.hub.InitiativeManager`.
        """
        return InitiativeManager(self)

    @_lazy_property
    def events(self):
        """
        The resource manager for Hub events. See :class:`~arcgis.apps.hub.EventManager`.
        """
        if self._hub_enabled:
            return EventManager(self)
        else:
            raise Exception(
                "Events is only available with Hub Premium. Please upgrade to Hub Premium to use this feature."
            )

    @_lazy_property
    def sites(self):
        """
        The resource manager for Hub sites. See :class:`~hub.sites.SiteManager`.
        """
        return SiteManager(self)

    @_lazy_property
    def pages(self):
        """
        The resource manager for Hub pages. See :class:`~hub.sites.PageManager`.
        """
        return PageManager(self)


class Initiative(OrderedDict):
    """
    Represents an initiative within a Hub. An Initiative supports
    policy- or activity-oriented goals through workflows, tools and team collaboration.
    """

    def __init__(self, gis, initiativeItem):
        """
        Constructs an empty Initiative object
        """
        self.item = initiativeItem
        self._gis = gis
        self._hub = gis.hub
        try:
            self._initiativedict = self.item.get_data()
            pmap = PropertyMap(self._initiativedict)
            self.definition = pmap
        except:
            self.definition = None

    def __repr__(self):
        return '<%s title:"%s" owner:%s>' % (
            type(self).__name__,
            self.title,
            self.owner,
        )

    @property
    def itemid(self):
        """
        Returns the item id of the initiative item
        """
        return self.item.id

    @property
    def title(self):
        """
        Returns the title of the initiative item
        """
        return self.item.title

    @property
    def description(self):
        """
        Returns the initiative description
        """
        return self.item.description

    @property
    def snippet(self):
        """
        Getter/Setter for the initiative snippet
        """
        return self.item.snippet

    @snippet.setter
    def snippet(self, value):
        self.item.snippet = value

    @property
    def owner(self):
        """
        Returns the owner of the initiative item
        """
        return self.item.owner

    @property
    def tags(self):
        """
        Returns the tags of the initiative item
        """
        return self.item.tags

    @property
    def url(self):
        """
        Returns the url of the initiative site
        """
        try:
            return self.item.properties["url"]
        except:
            return self.item.url

    @property
    def site_id(self):
        """
        Returns the itemid of the initiative site
        """
        try:
            return self.item.properties["siteId"]
        except:
            return self._initiativedict["steps"][0]["itemIds"][0]

    @property
    def site_url(self):
        """
        Getter/Setter for the url of the initiative site
        """
        try:
            return self.item.url
        except:
            return self.sites.get(self.site_id).url

    @site_url.setter
    def site_url(self, value):
        self.item.url = value

    @property
    def content_group_id(self):
        """
        Returns the groupId for the content group
        """
        return self.item.properties["contentGroupId"]

    @property
    def collab_group_id(self):
        """
        Getter/Setter for the groupId for the collaboration group
        """
        try:
            return self.item.properties["collaborationGroupId"]
        except:
            return None

    @collab_group_id.setter
    def collab_group_id(self, value):
        self.item.properties["collaborationGroupId"] = value

    @property
    def followers_group_id(self):
        """
        Returns the groupId for the followers group
        """
        return self.item.properties["followersGroupId"]

    @_lazy_property
    def sites(self):
        """
        The resource manager for an Initiative's sites.
        See :class:`~hub.sites.SiteManager`.
        """
        return SiteManager(self._hub, self)

    @_lazy_property
    def all_events(self):
        """
        Fetches all events (past or future) pertaining to an initiative
        """
        return self._gis.hub.events.search(initiative_id=self.item.id)

    @_lazy_property
    def followers(self):
        """
        Fetches the list of followers for initiative.
        """
        # Fetch followers group
        _followers_group = _followers_group = self._gis.groups.get(
            self.followers_group_id
        )
        return _followers_group.get_members()

    def add_content(self, items_list):
        """
        Adds a batch of items to the initiative content library.
        =====================     ====================================================================
        **Argument**              **Description**
        ---------------------     --------------------------------------------------------------------
        items_list                Required list. A list of Item or item ids to add to the initiative
        =====================     ====================================================================
        """
        # Fetch Initiative Collaboration group
        _collab_group = self._gis.groups.get(self.collab_group_id)
        # Fetch Content Group
        _content_group = self._gis.groups.get(self.content_group_id)
        # share items with groups
        return self._gis.content.share_items(
            items_list, groups=[_collab_group, _content_group]
        )

    def delete(self):
        """
        Deletes the initiative, its site and associated groups.
        If unable to delete, raises a RuntimeException.

        :return:
            A bool containing True (for success) or False (for failure).

        .. code-block:: python

            USAGE EXAMPLE: Delete an initiative successfully

            initiative1 = myHub.initiatives.get('itemId12345')
            initiative1.delete()

            >> True
        """
        if self.item is not None:
            # Fetch initiative site
            _site = self._gis.hub.sites.get(self.site_id)
            Site.delete(_site)
            # Fetch and delete Initiative Collaboration group if exists
            try:
                _collab_group = self._gis.groups.get(self.collab_group_id)
                _collab_group.protected = False
                _collab_group.delete()
            except:
                pass
            # Fetch Content Group
            _content_group = self._gis.groups.get(self.content_group_id)
            # Fetch Followers Group for Hub Premium initiatives
            if self._gis.hub._hub_enabled:
                _followers_group = self._gis.groups.get(self.followers_group_id)
            # Disable delete protection on groups
            try:
                _content_group.protected = False
                _followers_group.protected = False
                _followers_group.delete()
            except:
                pass
            # Delete groups and initiative
            _content_group.delete()
            return self.item.delete()

    def reassign_to(self, target_owner):
        """
        Allows the administrator to reassign the initiative object from one
        user to another.

        .. note::
            This will transfer ownership of all items (site, pages, content) and groups that
            belong to this initiative to the new target_owner.

        =====================     ====================================================================
        **Argument**              **Description**
        ---------------------     --------------------------------------------------------------------
        target_owner              Required string. The new desired owner of the initiative.
        =====================     ====================================================================
        """
        # check if admin user is performing this action
        if "admin" not in self._gis.users.me.role:
            return Exception(
                "You do not have the administrator privileges to perform this action."
            )
        # check if core team is needed by checking the role of the target_owner
        if self._gis.users.get(target_owner).role == "org_admin":
            # check if the initiative comes with core team by checking owner's role
            if self._gis.users.get(self.owner).role == "org_admin":
                # fetch the core team for the initative
                core_team = self._gis.groups.get(self.collab_group_id)
                # fetch the contents shared with this team
                core_team_content = core_team.content()
                # check if target_owner is part of core team, else add them to core team
                members = core_team.get_members()
                if (
                    target_owner not in members["admins"]
                    or target_owner not in members["users"]
                ):
                    core_team.add_users(target_owner)
                # remove items from core team
                self._gis.content.unshare_items(core_team_content, groups=[core_team])
                # reassign to target_owner
                for item in core_team_content:
                    item.reassign_to(target_owner)
                # fetch the items again since they have been reassigned
                new_content_list = []
                for item in core_team_content:
                    item_temp = self._gis.content.get(item.id)
                    new_content_list.append(item_temp)
                # share item back to the content group
                self._gis.content.share_items(
                    new_content_list, groups=[core_team], allow_members_to_edit=True
                )
                # reassign core team to target owner
                core_team.reassign_to(target_owner)
            else:
                # create core team necessary for the initiative
                _collab_group_title = title + " Core Team"
                _collab_group_dict = {
                    "title": _collab_group_title,
                    "tags": [
                        "Hub Group",
                        "Hub Initiative Group",
                        "Hub Site Group",
                        "Hub Core Team Group",
                        "Hub Team Group",
                    ],
                    "access": "org",
                    "capabilities": "updateitemcontrol",
                    "membershipAccess": "collaboration",
                    "snippet": "Members of this group can create, edit, and manage the site, pages, and other content related to hub-groups.",
                }
                collab_group = self._gis.groups.create_from_dict(_collab_group_dict)
                collab_group.protected = True
                self.collab_group_id = collab_group.id
        else:
            # reassign the initiative, site, page items
            self.item.reassign_to(target_owner)
            site = self._hub.sites.get(self.site_id)
            site.item.reassign_to(target_owner)
            site_pages = site.pages.search()
            # If pages exist
            if len(site_pages) > 0:
                for page in site_pages:
                    # Unlink page (deletes if)
                    page.item.reassign_to(target_owner)
        # fetch content group
        content_team = self._gis.groups.get(self.content_group_id)
        # reassign to target_owner
        content_team.reassign_to(target_owner)
        # If it is a Hub Premium initiative, repeat for followers group
        if self._hub._hub_enabled:
            followers_team = self._gis.groups.get(self.followers_group_id)
            followers_team.reassign_to(target_owner)
        return self._gis.content.get(self.itemid)

    def share(
        self, everyone=False, org=False, groups=None, allow_members_to_edit=False
    ):
        """
        Shares an initiative and associated site with the specified list of groups.

        ======================  ========================================================
        **Argument**            **Description**
        ----------------------  --------------------------------------------------------
        everyone                Optional boolean. Default is False, don't share with
                                everyone.
        ----------------------  --------------------------------------------------------
        org                     Optional boolean. Default is False, don't share with
                                the organization.
        ----------------------  --------------------------------------------------------
        groups                  Optional list of group ids as strings, or a list of
                                arcgis.gis.Group objects, or a comma-separated list of
                                group IDs.
        ----------------------  --------------------------------------------------------
        allow_members_to_edit   Optional boolean. Default is False, to allow item to be
                                shared with groups that allow shared update
        ======================  ========================================================

        :return:
            A dictionary with key "notSharedWith" containing array of groups with which the items could not be shared.
        """
        site = self._gis.sites.get(self.site_id)
        result1 = site.item.share(
            everyone=everyone,
            org=org,
            groups=groups,
            allow_members_to_edit=allow_members_to_edit,
        )
        result2 = self.item.share(
            everyone=everyone,
            org=org,
            groups=groups,
            allow_members_to_edit=allow_members_to_edit,
        )
        print(result1)
        return result2

    def unshare(self, groups):
        """
        Stops sharing of the initiative and its associated site with the specified list of groups.

        ================  =========================================================================================
        **Argument**      **Description**
        ----------------  -----------------------------------------------------------------------------------------
        groups            Optional list of group names as strings, or a list of arcgis.gis.Group objects,
                          or a comma-separated list of group IDs.
        ================  =========================================================================================

        :return:
            Dictionary with key "notUnsharedFrom" containing array of groups from which the items could not be unshared.
        """
        site = self._gis.sites.get(self.site_id)
        result1 = site.item.unshare(groups=groups)
        result2 = self.item.unshare(groups=groups)
        print(result1)
        return result2

    def update(
        self, initiative_properties=None, data=None, thumbnail=None, metadata=None
    ):
        """Updates the initiative.


        .. note::
            For initiative_properties, pass in arguments for only the properties you want to be updated.
            All other properties will be untouched.  For example, if you want to update only the
            initiative's description, then only provide the description argument in initiative_properties.


        =====================     ====================================================================
        **Argument**              **Description**
        ---------------------     --------------------------------------------------------------------
        initiative_properties     Required dictionary. See URL below for the keys and values.
        ---------------------     --------------------------------------------------------------------
        data                      Optional string. Either a path or URL to the data.
        ---------------------     --------------------------------------------------------------------
        thumbnail                 Optional string. Either a path or URL to a thumbnail image.
        ---------------------     --------------------------------------------------------------------
        metadata                  Optional string. Either a path or URL to the metadata.
        =====================     ====================================================================


        To find the list of applicable options for argument initiative_properties -
        https://esri.github.io/arcgis-python-api/apidoc/html/arcgis.gis.toc.html#arcgis.gis.Item.update

        :return:
           A boolean indicating success (True) or failure (False).

        .. code-block:: python

            USAGE EXAMPLE: Update an initiative successfully

            initiative1 = myHub.initiatives.get('itemId12345')
            initiative1.update(initiative_properties={'description':'Create your own initiative to organize people around a shared goal.'})

            >> True
        """
        if initiative_properties:
            _initiative_data = self.definition
            for key, value in initiative_properties.items():
                _initiative_data[key] = value
                if key == "title":
                    title = value
                    # Fetch Initiative Collaboration group
                    try:
                        _collab_group = self._gis.groups.get(self.collab_group_id)
                        _collab_group.update(title=title + " Core Team")
                    except:
                        pass
                    # Fetch Followers Group
                    try:
                        _followers_group = self._gis.groups.get(self.followers_group_id)
                        _followers_group.update(title=title + " Followers")
                    except:
                        pass
                    # Fetch Content Group
                    _content_group = self._gis.groups.get(self.content_group_id)
                    # Update title for group
                    _content_group.update(title=title + " Content")
            return self.item.update(_initiative_data, data, thumbnail, metadata)


class InitiativeManager(object):
    """
    Helper class for managing initiatives within a Hub. This class is not created by users directly.
    An instance of this class, called 'initiatives', is available as a property of the Hub object. Users
    call methods on this 'initiatives' object to manipulate (add, get, search, etc) initiatives.
    """

    def __init__(self, hub, initiative=None):
        self._hub = hub
        self._gis = self._hub.gis

    def add(self, title, description=None, site=None, data=None, thumbnail=None):
        """
        Adds a new initiative to the Hub.
        ===============     ====================================================================
        **Argument**        **Description**
        ---------------     --------------------------------------------------------------------
        title               Required string.
        ---------------     --------------------------------------------------------------------
        description         Optional string.
        ---------------     --------------------------------------------------------------------
        site                Optional Site object.
        ---------------     --------------------------------------------------------------------
        data                Optional string. Either a path or URL to the data.
        ---------------     --------------------------------------------------------------------
        thumbnail           Optional string. Either a path or URL to a thumbnail image.
        ===============     ====================================================================

        :return:
           The initiative if successfully added, None if unsuccessful.

        .. code-block:: python

            USAGE EXAMPLE: Add an initiative successfully

            initiative1 = myHub.initiatives.add(title='Vision Zero Analysis')
            initiative1.item
        """

        # Define initiative
        if description is None:
            description = "Create your own initiative by combining existing applications with a custom site."
        _snippet = "Create your own initiative by combining existing applications with a custom site. Use this initiative to form teams around a problem and invite your community to participate."
        _item_dict = {
            "type": "Hub Initiative",
            "snippet": _snippet,
            "typekeywords": "Hub, hubInitiative, OpenData",
            "title": title,
            "description": description,
            "licenseInfo": "CC-BY-SA",
            "culture": "{{culture}}",
            "properties": {},
        }

        # Defining content, collaboration and followers groups
        _content_group_title = title + " Content"
        _content_group_dict = {
            "title": _content_group_title,
            "tags": [
                "Hub Group",
                "Hub Content Group",
                "Hub Site Group",
                "Hub Initiative Group",
            ],
            "access": "public",
        }
        _collab_group_title = title + " Core Team"
        _collab_group_dict = {
            "title": _collab_group_title,
            "tags": [
                "Hub Group",
                "Hub Initiative Group",
                "Hub Site Group",
                "Hub Core Team Group",
                "Hub Team Group",
            ],
            "access": "org",
            "capabilities": "updateitemcontrol",
            "membershipAccess": "collaboration",
            "snippet": "Members of this group can create, edit, and manage the site, pages, and other content related to hub-groups.",
        }
        _followers_group_title = title + " Followers"
        _followers_group_dict = {
            "title": _followers_group_title,
            "tags": [
                "Hub Group",
                "Hub Initiative Group",
                " Hub Initiative Followers Group",
            ],
            "access": "public",
        }

        # Create groups
        content_group = self._gis.groups.create_from_dict(_content_group_dict)
        # Protect groups from accidental deletion
        content_group.protected = True
        # Adding it to _item_dict
        _item_dict["properties"]["contentGroupId"] = content_group.id
        if self._gis.users.me.role == "org_admin":
            collab_group = self._gis.groups.create_from_dict(_collab_group_dict)
            collab_group.protected = True
            _item_dict["properties"]["collaborationGroupId"] = collab_group.id
        if self._hub._hub_enabled:
            followers_group = self._gis.groups.create_from_dict(_followers_group_dict)
            followers_group.protected = True
            _item_dict["properties"]["followersGroupId"] = followers_group.id

        # Create initiative and share it with collaboration group
        item = self._gis.content.add(_item_dict, owner=self._gis.users.me.username)
        try:
            item.share(groups=[collab_group])
        except:
            pass

        # Create initiative site and set initiative properties
        _initiative = Initiative(self._gis, item)
        # If it is a brand new initiative, create new site
        if site is None:
            site = _initiative.sites.add(title=title)
        # else clone existing site
        else:
            site = _initiative.sites.clone(site, pages=True, title=title)
        item.update(
            item_properties={
                "url": site.url,
                "culture": self._gis.properties.user.culture,
            }
        )
        _initiative.site_url = site.item.url
        item.properties["site_id"] = site.itemid

        # update initiative data
        _item_data = {
            "assets": [
                {
                    "id": "bannerImage",
                    "properties": {
                        "type": "resource",
                        "fileName": "detail-image.jpg",
                        "mimeType": "image/jepg",
                    },
                    "license": {"type": "none"},
                    "display": {"position": {"x": "center", "y": "center"}},
                },
                {
                    "id": "iconDark",
                    "properties": {
                        "type": "resource",
                        "fileName": "icon-dark.png",
                        "mimeType": "image/png",
                    },
                    "license": {"type": "none"},
                },
                {
                    "id": "iconLight",
                    "properties": {
                        "type": "resource",
                        "fileName": "icon-light.png",
                        "mimeType": "image/png",
                    },
                    "license": {"type": "none"},
                },
            ],
            "steps": [
                {
                    "id": "informTools",
                    "title": "Inform the Public",
                    "description": "Share data about your initiative with the public so people can easily find, download and use your data in different formats.",
                    "templateIds": [],
                    "itemIds": [site.itemid],
                },
                {
                    "id": "listenTools",
                    "title": "Listen to the Public",
                    "description": "Create ways to gather citizen feedback to help inform your city officials.",
                    "templateIds": [],
                    "itemIds": [],
                },
                {
                    "id": "monitorTools",
                    "title": "Monitor Progress",
                    "description": "Establish performance measures that incorporate the publics perspective.",
                    "templateIds": [],
                    "itemIds": [],
                },
            ],
            "indicators": [],
            "values": {
                "bannerImage": {
                    "source": "bannerImage",
                    "display": {"position": {"x": "center", "y": "center"}},
                },
            },
        }
        _data = json.dumps(_item_data)
        item.update(item_properties={"text": _data})
        return Initiative(self._gis, item)

    def clone(self, initiative, origin_hub=None, title=None):
        """
        Clone allows for the creation of an initiative that is derived from the current initiative.

        .. note::
            If both your `origin_hub` and `destination_hub` are Hub Basic organizations, please use the
            `clone` method supported under the `~arcgis.apps.sites.SiteManager` class.

        ===============     ====================================================================
        **Argument**        **Description**
        ---------------     --------------------------------------------------------------------
        initiative          Required Initiative object of initiative to be cloned.
        ---------------     --------------------------------------------------------------------
        origin_hub          Optional Hub object. Required only for cross-org clones where the
                            initiative being cloned is not an item with public access.
        ---------------     --------------------------------------------------------------------
        title               Optional String.
        ===============     ====================================================================

        :return:
           Initiative.

        .. code-block:: python

            USAGE EXAMPLE: Clone an initiative in another organization

            #Connect to Hub
            hub_origin = gis1.hub
            hub_destination = gis2.hub
            #Fetch initiative
            initiative1 = hub_origin.initiatives.get('itemid12345')
            #Clone in another Hub
            initiative_cloned = hub_destination.initiatives.clone(initiative1, origin_hub=hub_origin)
            initiative_cloned.item


            USAGE EXAMPLE: Clone initiative in the same organization

            myhub = gis.hub
            initiative1 = myhub.initiatives.get('itemid12345')
            initiative2 = myhub.initiatives.clone(initiative1, title='New Initiative')

        """
        from datetime import timezone

        now = datetime.now(timezone.utc)
        # Checking if item of correct type has been passed
        if "hubInitiative" not in initiative.item.typeKeywords:
            raise Exception("Incorrect item type. Initiative item needed for cloning.")
        # Checking if initiative or site needs to be cloned
        if self._hub and origin_hub:
            if not self._hub._hub_enabled and not origin_hub._hub_enabled:
                raise Exception(
                    "For Hub Basic organizations, please clone the site instead of initiative."
                )
        # New title
        if title is None:
            title = initiative.title + "-copy-%s" % int(now.timestamp() * 1000)
        # If cloning within same org
        if origin_hub is None:
            origin_hub = self._hub
        # Fetch site (checking if origin_hub is correct or if initiative is public)
        try:
            site = origin_hub.sites.get(initiative.site_id)
        except:
            raise Exception(
                "Please provide origin_hub of the initiative object, if the initiative is not publicly shared"
            )
        # Create new initiative if destination hub is premium
        if self._hub._hub_enabled:
            # new initiative
            new_initiative = self._hub.initiatives.add(title=title, site=site)
            return new_initiative
        else:
            # Create new site if destination hub is basic/enterprise
            new_site = self._hub.sites.clone(site, pages=True, title=title)
            return new_site

    def get(self, initiative_id):
        """Returns the initiative object for the specified initiative_id.

        =======================    =============================================================
        **Argument**               **Description**
        -----------------------    -------------------------------------------------------------
        initiative_id              Required string. The initiative itemid.
        =======================    =============================================================

        :return:
            The initiative object if the item is found, None if the item is not found.

        .. code-block:: python

            USAGE EXAMPLE: Fetch an initiative successfully

            initiative1 = myHub.initiatives.get('itemId12345')
            initiative1.item

        """
        initiativeItem = self._gis.content.get(initiative_id)
        if "hubInitiative" in initiativeItem.typeKeywords:
            return Initiative(self._gis, initiativeItem)
        else:
            raise TypeError("Item is not a valid initiative or is inaccessible.")

    def search(
        self, scope=None, title=None, owner=None, created=None, modified=None, tags=None
    ):
        """
        Searches for initiatives.

        ===============     ====================================================================
        **Argument**        **Description**
        ---------------     --------------------------------------------------------------------
        scope               Optional string. Defines the scope of search.
                            Valid values are 'official', 'community' or 'all'.
        ---------------     --------------------------------------------------------------------
        title               Optional string. Return initiatives with provided string in title.
        ---------------     --------------------------------------------------------------------
        owner               Optional string. Return initiatives owned by a username.
        ---------------     --------------------------------------------------------------------
        created             Optional string. Date the initiative was created.
                            Shown in milliseconds since UNIX epoch.
        ---------------     --------------------------------------------------------------------
        modified            Optional string. Date the initiative was last modified.
                            Shown in milliseconds since UNIX epoch
        ---------------     --------------------------------------------------------------------
        tags                Optional string. User-defined tags that describe the initiative.
        ===============     ====================================================================

        :return:
           A list of matching initiatives.
        """

        initiativelist = []

        # Build search query
        query = "typekeywords:hubInitiative"
        if title != None:
            query += " AND title:" + title
        if owner != None:
            query += " AND owner:" + owner
        if created != None:
            query += " AND created:" + created
        if modified != None:
            query += " AND modified:" + modified
        if tags != None:
            query += " AND tags:" + tags

        # Apply org scope and search
        if scope is None or self._gis.url == "https://www.arcgis.com":
            items = self._gis.content.search(query=query, max_items=5000)
        elif scope.lower() == "official":
            query += " AND access:public"
            _gis = GIS(self._hub.enterprise_org_url)
            items = _gis.content.search(query=query, max_items=5000)
        elif scope.lower() == "community":
            query += " AND access:public"
            _gis = GIS(self._hub.community_org_url)
            items = _gis.content.search(query=query, max_items=5000)
        elif scope.lower() == "all":
            items = self._gis.content.search(
                query=query, outside_org=True, max_items=5000
            )
        else:
            raise Exception("Invalid value for scope")

        # Return searched initiatives
        for item in items:
            initiativelist.append(Initiative(self._gis, item))
        return initiativelist


class Event(OrderedDict):
    """
    Represents an event in a Hub. A Hub has many Events that can be associated with an Initiative.
    Events are meetings for people to support an Initiative. Events are scheduled by an organizer
    and have many attendees. An Event has a Group so that they can include content for preparation
    as well as gather and archive content during the event for later retrieval or analysis.
    """

    def __init__(self, gis, eventObject):
        """
        Constructs an empty Event object
        """
        self._gis = gis
        self._hub = self._gis.hub
        self._eventdict = eventObject["attributes"]
        try:
            self._eventdict["geometry"] = eventObject["geometry"]
        except KeyError:
            self._eventdict["geometry"] = {"x": 0.00, "y": 0.00}
        pmap = PropertyMap(self._eventdict)
        self.definition = pmap

    def __repr__(self):
        return '<%s title:"%s" venue:%s>' % (
            type(self).__name__,
            self.title,
            self.venue,
        )

    @property
    def event_id(self):
        """
        Returns the unique identifier of the event
        """
        return self._eventdict["OBJECTID"]

    @property
    def title(self):
        """
        Returns the title of the event
        """
        return self._eventdict["title"]

    @property
    def venue(self):
        """
        Returns the location of the event
        """
        return self._eventdict["venue"]

    @property
    def address(self):
        """
        Returns the street address for the venue of the event
        """
        return self._eventdict["address1"]

    @property
    def initiative_id(self):
        """
        Returns the initiative id of the initiative the event belongs to
        """
        return self._eventdict["initiativeId"]

    @property
    def organizers(self):
        """
        Returns the name and email of the event organizers
        """
        return self._eventdict["organizers"]

    @property
    def description(self):
        """
        Returns description of the event
        """
        return self._eventdict["description"]

    @property
    def start_date(self):
        """
        Returns start date of the event in milliseconds since UNIX epoch
        """
        return self._eventdict["startDate"]

    @property
    def end_date(self):
        """
        Returns end date of the event in milliseconds since UNIX epoch
        """
        return self._eventdict["endDate"]

    @property
    def creator(self):
        """
        Returns creator of the event
        """
        return self._eventdict["Creator"]

    @property
    def capacity(self):
        """
        Returns attendance capacity for attendees of the event
        """
        return self._eventdict["capacity"]

    @property
    def attendance(self):
        """
        Returns attendance count for a past event
        """
        return self._eventdict["attendance"]

    @property
    def access(self):
        """
        Returns access permissions of the event
        """
        return self._eventdict["status"]

    @property
    def group_id(self):
        """
        Returns groupId for the event
        """
        return self._eventdict["groupId"]

    @property
    def is_cancelled(self):
        """
        Check if event is Cancelled
        """
        return self._eventdict["isCancelled"]

    @property
    def geometry(self):
        """
        Returns co-ordinates of the event location
        """
        return self._eventdict["geometry"]

    def delete(self):
        """
        Deletes an event

        :return:
            A bool containing True (for success) or False (for failure).

        .. code-block:: python

            USAGE EXAMPLE: Delete an event successfully

            event1 = myhub.events.get(24)
            event1.delete()

            >> True
        """
        _group = self._gis.groups.get(self.group_id)
        _group.protected = False
        _group.delete()
        params = {
            "f": "json",
            "objectIds": self.event_id,
            "token": self._gis._con.token,
        }
        delete_event = self._gis._con.post(
            path="https://hub.arcgis.com/api/v3/events/"
            + self._hub.enterprise_org_id
            + "/Hub Events/FeatureServer/0/deleteFeatures",
            postdata=params,
        )
        return delete_event["deleteResults"][0]["success"]

    def update(self, event_properties):
        """
        Updates properties of an event

        :return:
            A bool containing True (for success) or False (for failure).

        .. code-block:: python

            USAGE EXAMPLE: Update an event successfully

            event1 = myhub.events.get(id)
            event_properties = {'status': 'planned', description: 'Test'}
            event1.update(event_properties)

            >> True
        """
        _feature = {}

        # Build event feature
        event_properties["OBJECTID"] = self.event_id
        _feature["attributes"] = self._eventdict
        for key, value in event_properties.items():
            _feature["attributes"][key] = value
        _feature["geometry"] = self.geometry
        event_data = [_feature]

        # Update event
        url = (
            "https://hub.arcgis.com/api/v3/events/"
            + self._hub.enterprise_org_id
            + "/Hub Events/FeatureServer/0/updateFeatures"
        )
        params = {"f": "json", "features": event_data, "token": self._gis._con.token}
        update_event = self._gis._con.post(path=url, postdata=params)
        return update_event["updateResults"][0]["success"]


class EventManager(object):
    """Helper class for managing events within a Hub. This class is not created by users directly.
    An instance of this class, called 'events', is available as a property of the Hub object. Users
    call methods on this 'events' object to manipulate (add, search, get_map etc) events
    of a particular Hub.
    """

    def __init__(self, hub, event=None):
        self._hub = hub
        self._gis = self._hub.gis
        if event:
            self._event = event

    def _all_events(self):
        """
        Fetches all events for particular hub.
        """
        events = []
        url = (
            "https://hub.arcgis.com/api/v3/events/"
            + self._hub.enterprise_org_id
            + "/Hub Events/FeatureServer/0/query"
        )
        params = {
            "f": "json",
            "outFields": "*",
            "where": "1=1",
            "token": self._gis._con.token,
        }
        all_events = self._gis._con.get(url, params)
        _events_data = all_events["features"]
        for event in _events_data:
            events.append(Event(self._gis, event))
        return events

    def add(self, event_properties):
        """
        Adds an event for an initiative.

        ===============     ====================================================================
        **Argument**        **Description**
        ---------------     --------------------------------------------------------------------
        event_properties    Required dictionary. See table below for the keys and values.
        ===============     ====================================================================


        *Key:Value Dictionary Options for Argument event_properties*
        =================  =====================================================================
        **Key**            **Value**
        -----------------  ---------------------------------------------------------------------
        title              Required string. Name of event.
        -----------------  ---------------------------------------------------------------------
        description        Required string. Description of the event.
        -----------------  ---------------------------------------------------------------------
        initiaitve_id      Required string. Name label of the item.
        -----------------  ---------------------------------------------------------------------
        venue              Required string. Venue name for the event.
        -----------------  ---------------------------------------------------------------------
        address1           Required string. Street address for the venue.
        -----------------  ---------------------------------------------------------------------
        status             Required string. Access of event. Valid values are private, planned,
                           public, draft.
        -----------------  ---------------------------------------------------------------------
        startDate          Required start date of the event in milliseconds since UNIX epoch.
        -----------------  ---------------------------------------------------------------------
        endDate            Required end date of the event in milliseconds since UNIX epoch.
        -----------------  ---------------------------------------------------------------------
        isAllDay           Required boolean. Indicates if the event is a day long event.
        -----------------  ---------------------------------------------------------------------
        capacity           Optional integer. The attendance capacity of the event venue.
        -----------------  ---------------------------------------------------------------------
        address2           Optional string.  Additional information about event venue street address.
        -----------------  ---------------------------------------------------------------------
        onlineLocation     Optional string. Web URL or other details for online event.
        -----------------  ---------------------------------------------------------------------
        organizers         Optional list of dictionary of keys `name` and `contact` for each organizer's
                           name and email. Default values are name, email, username of event creator.
        -----------------  ---------------------------------------------------------------------
        sponsors           Optional list of dictionary of keys `name` and `contact` for each sponsor's
                           name and contact.
        =================  =====================================================================

        :return:
            Event if successfully added.

        .. code-block:: python

            USAGE EXAMPLE: Add an event successfully

            event_properties = {
                'title':'Test Event',
                'description': 'Testing with python',
                'initiativeId': '43f..',
                'venue': 'Washington Monument',
                'address1': '2 15th St NW, Washington, District of Columbia, 20024',
                'status': 'planned',
                'startDate': 1562803200,
                'endDate': 1562889600,
                'isAllDay': 1
            }

            new_event = myhub.events.add(event_properties)
        """
        _feature = {}
        # Fetch initiaitve site id
        _initiative = self._hub.initiatives.get(event_properties["initiativeId"])
        event_properties["siteId"] = _initiative.site_id
        # Set organizers if not provided
        try:
            event_properties["organizers"]
        except:
            _organizers_list = [
                {
                    "name": self._gis.users.me.fullName,
                    "contact": self._gis.users.me.email,
                    "username": self._gis.users.me.username,
                }
            ]
            _organizers = json.dumps(_organizers_list)
            event_properties["organizers"] = _organizers
        # Set sponsors if not provided
        try:
            event_properties["sponsors"]
            event_properties["sponsors"] = json.dumps(event_properties["sponsors"])
        except:
            _sponsors = []
            event_properties["sponsors"] = json.dumps(_sponsors)
        # Set onlineLocation if not provided
        try:
            event_properties["onlineLocation"]
        except:
            _onlineLocation = ""
            event_properties["onlineLocation"] = _onlineLocation
        # Set geometry if not provided
        try:
            event_properties["geometry"]
            geometry = event_properties["geometry"]
            del event_properties["geometry"]
        except:
            geometry = geocode(event_properties["address1"])[0]["location"]

        event_properties["schemaVersion"] = 2
        event_properties["location"] = ""
        event_properties["url"] = event_properties["title"].replace(" ", "-").lower()

        # Generate event id for new event
        event_id = max([event.event_id for event in self._all_events()]) + 1

        # Create event group
        _event_group_dict = {
            "title": event_properties["title"],
            "access": "public",
            "tags": ["Hub Event Group", "Open Data", "hubEvent|" + str(event_id)],
        }
        _event_group = self._gis.groups.create_from_dict(_event_group_dict)
        _event_group.protected = True
        event_properties["groupId"] = _event_group.id

        # Build new event feature and create it
        _feature["attributes"] = event_properties
        _feature["geometry"] = geometry
        event_data = [_feature]
        url = (
            "https://hub.arcgis.com/api/v3/events/"
            + self._hub.enterprise_org_id
            + "/Hub Events/FeatureServer/0/addFeatures"
        )
        params = {"f": "json", "features": event_data, "token": self._gis._con.token}
        add_event = self._gis._con.post(path=url, postdata=params)
        try:
            add_event["addResults"]
            return self.get(add_event["addResults"][0]["objectId"])
        except:
            return add_event

    def search(self, initiative_id=None, title=None, venue=None, organizer_name=None):
        """
        Searches for events within a Hub.

        ===============     ====================================================================
        **Argument**        **Description**
        ---------------     --------------------------------------------------------------------
        initiative_id       Optional string. Initiative itemid.
        ---------------     --------------------------------------------------------------------
        title               Optional string. Title of the event.
        ---------------     --------------------------------------------------------------------
        venue               Optional string. Venue where event is held.
        ---------------     --------------------------------------------------------------------
        organizer_name      Optional string. Name of the organizer of the event.
        ===============     ====================================================================

        :return:
           A list of matching indicators.

        """
        events = []
        events = self._all_events()
        if initiative_id != None:
            # events =
            events = [event for event in events if initiative_id == event.initiative_id]
        if title != None:
            events = [event for event in events if title in event.title]
        if venue != None:
            events = [event for event in events if venue in event.venue]
        if organizer_name != None:
            events = [event for event in events if organizer_name in event.organizers]
        return events

    def get(self, event_id):
        """Get the event for the specified event_id.

        =======================    =============================================================
        **Argument**               **Description**
        -----------------------    -------------------------------------------------------------
        event_id                   Required integer. The event identifier.
        =======================    =============================================================

        :return:
            The event object.

        """
        url = (
            "https://hub.arcgis.com/api/v3/events/"
            + self._hub.enterprise_org_id
            + "/Hub Events/FeatureServer/0/"
            + str(event_id)
        )
        params = {"f": "json", "token": self._gis._con.token}
        feature = self._gis._con.get(url, params)
        return Event(self._gis, feature["feature"])
