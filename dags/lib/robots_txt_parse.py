import re
import requests
from datetime import datetime


def init(site_config):
    if site_config.get("ignore_robots_txt", False):
        return False

    ts = datetime.now().timestamp()
    if site_config['url'].startswith('https://water.europa.eu'):
        robots_url = site_config.get(
            "robots_txt", f"https://water.europa.eu/robots.txt?ts={ts}")
    else:
        robots_url = site_config.get(
            "robots_txt", f"{site_config['url']}/robots.txt?ts={ts}")
    print(robots_url)
    robots_txt = fetch_robots_txt(robots_url)
    disallow_by_robots = parse_robots(robots_txt)
    print("disallow_by_robots")
    print(disallow_by_robots)

    disallow_custom = []

    if site_config.get("custom_robots_txt", False) and site_config["custom_robots_txt"].get("rule", False):
        disallow_custom = site_config["custom_robots_txt"]['rule']

    print("disallow_custom")
    print(disallow_custom)

    disallow_patterns = disallow_by_robots + disallow_custom
    print("robots txt disallow patterns")
    print(disallow_patterns)
    if 0 == len(disallow_patterns):
        return False
    return disallow_patterns


def test_url(disallow_patterns, url):
    if not disallow_patterns:
        return True
    else:
        # temporarily only index /en for cca
        if url.startswith("https://climate-adapt.eea.europa.eu") and not url.startswith("https://climate-adapt.eea.europa.eu/en"):
            return False
        return is_allowed(url, disallow_patterns)


def fetch_robots_txt(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        raise ValueError("Could not fetch robots.txt")


def parse_robots(robots_txt):
    disallow_patterns = []
    for line in robots_txt.splitlines():
        line = line.strip()
        # Check if line starts with Disallow and extract path
        if line.startswith("Disallow:"):
            # Get the path after "Disallow: " and add it to the list
            path = line.split(":")[1].strip()
            if path:
                # Convert to regex pattern to handle wildcards
                pattern = re.escape(path).replace(r"\*", ".*")
                disallow_patterns.append(pattern)
    return disallow_patterns


def is_allowed(url_path, disallow_patterns):
    # Check if the URL path matches any of the disallow patterns
    for pattern in disallow_patterns:
        if re.fullmatch(pattern, url_path):
            return False
    return True
