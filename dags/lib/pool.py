from urllib.parse import urlparse

from lib.debug import pretty_id


def url_to_pool(url):
    return "p-{}".format(pretty_id(urlparse(url).hostname))[:49]
