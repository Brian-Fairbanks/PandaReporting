import typing
import urllib.parse as urllib_parse
import urllib.request
from functools import lru_cache


@lru_cache(maxsize=255)
def parse_url(url: str) -> object:
    """
    Parses a URL string into it's pieces.

    :returns: Named Tuple
    """
    return urllib_parse.urlparse(url)


def detect_proxy(replace_https: bool = True) -> typing.Optional[typing.Dict]:
    """
    Using `urllib.request.getproxies` create the dictionary for the proxy if they exist.

    .. note::
        This method cannot detect all proxies and is only recommended to assist if proxy
        errors occur. Talk to whomever manages your proxy to get the proper forward
        proxy information.

    .. note::
        See the urllib.requests.getproxies() `documentation
        <https://docs.python.org/3/library/urllib.request.html#urllib.request.getproxies>`_
         page for a full explination of the code.

    ===============     ====================================================================
    **Argument**        **Description**
    ---------------     --------------------------------------------------------------------
    replace_https       Optional Boolean.  The autodetect method from `urllib.requests.getproxies`
                        assumes there is an `http` and `https` version of the proxy.  Many
                        implementations of proxies force `https` traffic through the `http`
                        proxy endpoint.  When this is true, the `getproxies` call will be
                        modified for the `https` entry and switch the scheme from `https`
                        to `http`.
    ===============     ====================================================================

    Usage Example:

       gis = GIS(proxy=detect_proxy(True), verify_cert=False)
       gis = GIS(proxy=detect_proxy(False), verify_cert=False)

    :returns: Dict[str,str] or None
    """
    proxies = urllib.request.getproxies()
    if proxies and replace_https:
        if "https" in proxies:
            proxies["https"] = proxies["https"].replace("https", "http")
        return proxies
    elif proxies and replace_https == False:
        return proxies
    elif proxies == {}:
        proxies = None
    else:
        proxies = None
    return proxies


def merge_proxies(
    proxy_dict: typing.Dict[str, str] = None,
    proxy_host: str = None,
    proxy_port: str = "8888",
    detect: bool = False,
    replace_https: bool = False,
) -> typing.Dict[str, str]:
    """
    The `merge_proxies` combines multiple proxies based on the Python
    API's logical workflow.  The `proxy_host` and `proxy_port` are first
    considered, then the system detected proxies from
    `urllib.request.getproxies`, and finally `proxy_dict`.  These
    dictionaries and then merged together.  If the save `keys` exist, then
    they are overwritten.


    .. note::

        Helper utility to assist with debugging/trouble shooting proxy issues.
        This method is not intended to be used for public consumption and may
        change in the future.

    .. code-block:: python

        # Usage Example: proxy_dict, host/port and do not detect proxies

        >>> proxy_dict = {'https' : 'https://127.0.0.1:8734'}
        >>> proxy_host = "localhost"
        >>> proxy_host = "9000"

        >>> print(merge_proxies(proxy_dict=proxy_dict, proxy_host=proxy_host, proxy_port=proxy_port))
        {'https' : 'https://127.0.0.1:8734', 'http' : "http://localhost:9000"}


    :returns: Dict[str,str]

    """
    if proxy_dict is None and proxy_host is None and detect == False:
        return None
    if proxy_host and proxy_port:
        default_proxies = {
            "http": f"http://{proxy_host}:{proxy_port}",
            "https": f"https://{proxy_host}:{proxy_port}",
        }
    else:
        default_proxies = {}
    if detect:
        dectected_proxies = detect_proxy(replace_https)
    else:
        dectected_proxies = {}
    if proxy_dict is None:
        proxy_dict = {}

    default_proxies.update(dectected_proxies)
    default_proxies.update(proxy_dict)
    if default_proxies == {}:
        return None
    return default_proxies
