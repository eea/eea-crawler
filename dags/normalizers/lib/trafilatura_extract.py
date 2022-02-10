import logging

import lxml.html
import trafilatura

logger = logging.getLogger(__file__)


def remove_element(el):
    parent = el.getparent()
    if el.tail and el.tail.strip():
        prev = el.getprevious()
        if prev:
            prev.tail = (prev.tail or "") + el.tail
        else:
            parent.text = (parent.text or "") + el.tail
    parent.remove(el)


def get_text_from_html(html, config):
    if not html or len(html) == 0:
        return html

    e = lxml.html.fromstring(html)
    selectors = config.get("remove_by_selector", [])
    for selector in selectors:
        elements = e.cssselect(selector)
        for el in elements:
            try:
                remove_element(el)
            except Exception:
                logger.exception(
                    "Could not remove an element with selector: %s", selector
                )

    cleaned = lxml.html.tostring(e)
    text = trafilatura.extract(cleaned, favor_recall=True) or ""
    return text
