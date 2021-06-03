import re
from urllib.parse import urlparse


def pretty_id(value):
    return re.sub("[^a-zA-Z0-9\n.]", "_", value)


def hostname(url):
    parsed = urlparse(url)
    return parsed.hostname
