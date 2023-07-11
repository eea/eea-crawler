from usp.tree import sitemap_tree_for_homepage

def get_docs(site):
    tree = sitemap_tree_for_homepage(site)
    for doc in tree.all_pages():
        yield({'url':doc.url, 'last_modified':doc.last_modified})