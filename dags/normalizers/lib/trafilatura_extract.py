import logging
import json
import lxml.html
import trafilatura

logger = logging.getLogger(__file__)

patched_BODY_XPATH = [
    '''.//*[(self::article or self::div or self::main or self::section)][
    @class="post" or @class="entry" or
    contains(@class, "post-text") or contains(@class, "post_text") or
    contains(@class, "post-body") or contains(@class, "post-entry") or contains(@class, "postentry") or
    contains(@class, "post-content") or contains(@class, "post_content") or
    contains(@class, "postcontent") or contains(@class, "postContent") or
    contains(@class, "article-text") or contains(@class, "articletext") or contains(@class, "articleText")
    or contains(@id, "entry-content") or
    contains(@class, "entry-content") or contains(@id, "article-content") or
    contains(@class, "article-content") or contains(@id, "article__content") or
    contains(@class, "article__content") or contains(@id, "article-body") or
    contains(@class, "article-body") or contains(@id, "article__body") or
    contains(@class, "article__body") or @itemprop="articleBody" or
    contains(translate(@id, "B", "b"), "articlebody") or contains(translate(@class, "B", "b"), "articleBody")
    or @id="articleContent" or contains(@class, "ArticleContent") or
    contains(@class, "page-content") or contains(@class, "text-content") or
    contains(@id, "body-text") or contains(@class, "body-text") or
    contains(@class, "article__container") or contains(@id, "art-content") or contains(@class, "art-content")][1]''',
    # (…)[1] = first occurrence
    '(.//article)[1]',
    """(.//*[(self::article or self::div or self::main or self::section)][
    contains(@class, 'post-bodycopy') or
    contains(@class, 'storycontent') or contains(@class, 'story-content') or
    @class='postarea' or @class='art-postcontent' or
    contains(@class, 'theme-content') or contains(@class, 'blog-content') or
    contains(@class, 'section-content') or contains(@class, 'single-content') or
    contains(@class, 'single-post') or
    contains(@class, 'main-column') or contains(@class, 'wpb_text_column') or
    starts-with(@id, 'primary') or starts-with(@class, 'article ') or @class="text" or
    @id="article" or @class="cell" or @id="story" or @class="story" or
    contains(@class, "story-body") or contains(@class, "field-body") or
    contains(translate(@class, "FULTEX","fultex"), "fulltext")
    or @role='article'])[1]""",
    '''(.//*[(self::article or self::div or self::main or self::section)][
    contains(@id, "content-main") or contains(@class, "content-main") or contains(@class, "content_main") or
    contains(@id, "content-body") or contains(@class, "content-body") or contains(@id, "contentBody") or
    contains(@class, "content-area")
    or contains(@class, "content__body") or contains(translate(@id, "CM","cm"), "main-content") or contains(translate(@class, "CM","cm"), "main-content")
    or contains(translate(@class, "CP","cp"), "page-content") or
    @id="content" or @class="content"])[1]''',
    '(.//*[(self::article or self::div or self::section)][starts-with(@class, "main") or starts-with(@id, "main") or starts-with(@role, "main")])[1]|(.//main)[1]',
]

trafilatura.xpaths.BODY_XPATH[:] = patched_BODY_XPATH


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
    # logger.info("trafilatura extract get_text_from_html config type: %s, value is: %s", type(
    #     config), json.dumps(config))
    logger.info("TRAFILATURA DOCUMENT")
    if not html or len(html) == 0:
        logger.info("TRAFILATURA DOCUMENT EMPTY")
        return html

    main_by_css_selector = config.get("main_by_css_selector", None)
    # main_by_css_selector = '.db-item-view .col-left'
    logger.info('TRAFILATURA main css selecteor: %s', main_by_css_selector)

    # logger.info("TRAFILATURA LENGTH: %d and value is: %s",
    #             len(html), html)
    e = lxml.html.fromstring(html)
    if main_by_css_selector:
        matches = e.cssselect(main_by_css_selector)
        if matches:
            e = matches[0]
        else:
            logger.error(
                "trafilatura not found main_by_css_selector: %s", main_by_css_selector)
            # If not found return empty string
            return ''

    selectors = config.get("remove_by_selector", [])
    logger.info("remove_by_selector")
    logger.info(selectors)
    for selector in selectors:
        elements = e.cssselect(selector)
        for el in elements:
            try:
                el.getparent().remove(el)
                # remove_element(el)
                logger.info("remove an element with selector: %s", selector)
            except Exception:
                logger.exception(
                    "Could not remove an element with selector: %s", selector
                )

    skip_extract_with_trafilatura = config.get(
        "skip_extract_with_trafilatura", None)

    if skip_extract_with_trafilatura:
        text_elements = collect_leaf_elements_text(e)
        lines = []
        for text_element in text_elements:
            lines.append(text_element)

        text = ' '.join(filter(None, lines))
        logger.info("TRAFILATURA skip_extract_with_trafilatura type: %s, length: %d and value is: %s",
                    type(e), len(text), text)
        return text

    cleaned = lxml.html.tostring(e)
    text = trafilatura.extract(cleaned, favor_recall=True) or ""
    logger.info("TRAFILATURA DOCUMENT END")
    return text


def get_title_from_html(html, config):
    try:
        return (lxml.html.fromstring(html).find(".//title").text)
    except Exception:
        return config.get('fallback_title')


# def collect_elements(element, collected=None):
#     if collected is None:
#         collected = []

#     for child in element:
#         collected.append(child)
#         collect_elements(child, collected)

#     return collected


# def collect_leaf_elements(element, collected=None):
#     if collected is None:
#         collected = []

#     # Check if element has any child elements (tags)
#     has_child_tags = any(isinstance(child.tag, str) for child in element)

#     if not has_child_tags:
#         collected.append(element)
#     else:
#         for child in element:
#             collect_leaf_elements(child, collected)

#     return collected


def collect_leaf_elements_text(element, collected=None):
    if collected is None:
        collected = []

    # Check if element has any child elements (tags)
    has_child_tags = any(isinstance(child.tag, str) for child in element)

    if element.text:
        collected.append(element.text.strip())

    if has_child_tags:
        for child in element:
            collect_leaf_elements_text(child, collected)
            if child.tail:
                collected.append(child.tail.strip())

    return collected
