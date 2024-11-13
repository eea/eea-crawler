import urllib.robotparser
from datetime import datetime

from urllib.parse import unquote
import urllib.parse
import fnmatch


class RuleLine():
    """A rule line is a single "Allow:" (allowance==True) or "Disallow:"
       (allowance==False) followed by a path."""

    def __init__(self, path, allowance):
        if path == '' and not allowance:
            # an empty value means allow all
            allowance = True
        path = urllib.parse.urlunparse(urllib.parse.urlparse(path))
        self.path = unquote(urllib.parse.quote(path))
        self.allowance = allowance

    def applies_to(self, filename):
        self.path = unquote(self.path)
        if '*' in self.path:
            # print("RuleLine RobotsTxt: {} : {} : {}".format(filename, self.path,
            #       len(fnmatch.filter([filename], self.path))))
            return True if len(fnmatch.filter([filename], self.path)) else False
        return self.path == "*" or filename.startswith(self.path)

    def __str__(self):
        return ("Allow" if self.allowance else "Disallow") + ": " + self.path


urllib.robotparser.RuleLine = RuleLine


def init(site_config):
    if site_config.get("ignore_robots_txt", False):
        return False

    allowed_items = []
    ts = datetime.now().timestamp()
    if site_config['url'].startswith('https://water.europa.eu'):
        robots_url = site_config.get(
            "robots_txt", f"https://water.europa.eu/robots.txt?ts={ts}")
    else:
        robots_url = site_config.get(
            "robots_txt", f"{site_config['url']}/robots.txt?ts={ts}")
    print(robots_url)
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    return rp


def test_url(rp, url):
    if not rp:
        return True
    else:
        # temporarily only index /en for cca
        if url.startswith("https://climate-adapt.eea.europa.eu") and not url.startswith("https://climate-adapt.eea.europa.eu/en"):
            return False
        return rp.can_fetch("*", url)
