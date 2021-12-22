import trafilatura


def get_text_from_html(html, config):
    if not html or len(html) == 0:
        return html
    # TODO: apply config before extracting the text
    # print("TRAFILATURA CONFIG")
    # print(config)
    text = trafilatura.extract(html)
    return text
