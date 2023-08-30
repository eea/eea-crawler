import logging

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

def get_title_from_html(html, config):
    try:
        return (lxml.html.fromstring(html).find(".//title").text)
    except Exception:
        return config.get('fallback_title')
