from urllib import robotparser
from datetime import datetime

def init(site_config):
    if site_config.get("ignore_robots_txt", False):
        return False

    allowed_items = []
    ts = datetime.now().timestamp()
    if site_config['url'].startswith('https://water.europa.eu'):
        robots_url = site_config.get("robots_txt", f"https://water.europa.eu/robots.txt?ts={ts}")
    else:
        robots_url = site_config.get("robots_txt", f"{site_config['url']}/robots.txt?ts={ts}")
    print(robots_url)
    rp = robotparser.RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    return rp


def test_url(rp, url):
    if not rp:
        return True
    else:
        #temporarily only index /en for cca
        if url.startswith("https://climate-adapt.eea.europa.eu") and not url.startswith("https://climate-adapt.eea.europa.eu/en"):
            return False
        return rp.can_fetch("*", url)
