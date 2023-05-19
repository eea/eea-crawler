from urllib import robotparser


def init(site_config):
    if site_config.get("ignore_robots_txt", False):
        return False

    allowed_items = []
    robots_url = site_config.get("robots_txt", f"{site_config['url']}/robots.txt")
    print(robots_url)
    rp = robotparser.RobotFileParser()
    rp.set_url(robots_url)
    rp.read()
    return rp


def test_url(rp, url):
    if not rp:
        return True
    else:
        return rp.can_fetch("*", url)
