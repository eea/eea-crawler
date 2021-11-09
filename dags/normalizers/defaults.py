normalizers = {
    "//": "Properties that are indexed into ElasticSearch",
    "proplist": [
        "id",
        "about",
        "language",
        "title",
        "description",
        "themes",
        "subject",
        "location",
        "year",
        "modified",
        "site",
        "indexed_at",
    ],
    "//": "Normalise Properties, when you want to use a property instead of another",
    "normProp": {
        "temporalCoverage": ["time_coverage"],
        "title": ["title", "label"],
        "expirationDate": "expires",
        "location": "spatial",
        "effective": ["issued", "year"],
        "effectiveDate": ["issued", "year"],
        "@type": "objectProvides",
        "about": ["id", "about"],
        "themes": "topic",
        "description.data": "description",
    },
    "//": "Do not index these values for a given properties",
    "blackMap": {
        "expires": ["None", "Unknown"],
        "temporalCoverage": ["-1"],
        "year": ["None", "Unknown"],
        "effectiveDate": ["None", "Unknown"],
    },
    "//": "Index _only_ these values for given properties",
    "whiteMap": {},
    "//": "Normalise these following objects to a given value",
    "normObj": {
        "air": "Air pollution",
        "policy": "Policy instruments",
        "climate": "Climate change mitigation",
        "sustainability-transitions": "Sustainability transitions",
        "human": "Environment and health ",
        "waste": "Resource efficiency and waste",
        "energy": "Energy",
        "water": "Water and marine environment",
        "transport": "Transport",
        "biodiversity": "Biodiversity - Ecosystems",
        "climate-change-adaptation": "Climate change adaptation",
        "agriculture": "Agriculture",
        "landuse": "Land use",
        "default": "Various other issues",
        "chemicals": "Chemicals",
        "regions": "Specific regions",
        "industry": "Industry",
        "coast_sea": "Marine",
        "soil": "Soil",
        "Highlight": "News",
        "technology": "Environmental technology",
        "Term": "Glossary term",
        "File": "File",
        "EEAFigure": "Figure (chart/map)",
        "Document": "Webpage",
        "Page": "Webpage",
        "DavizVisualization": "Chart (interactive)",
        "ExternalDataSpec": "External data reference",
        "Report": "Report",
        "News": "News",
        "Folder": "Webpage",
        "AssessmentPart": "Indicator",
        "Organisation": "Organisation",
        "Fiche": "Briefing",
        "Article": "Article",
        "Data": "Data set",
        "Infographic": "Infographic",
        "Dashboard": "Data dashboard",
        "Assessment": "Indicator",
        "helpcenter_faq": "FAQ",
        "GIS Application": "Map (interactive)",
        "map_interactive": "Map (interactive)",
        "CloudVideo": "Video",
        "CallForTender": "Contract opportunity",
        "CallForInterest": "Contract opportunity",
        "CallForProposal": "Contract opportunity",
        "country_profile": "Country fact sheet",
    },
    "//": "Normalise missing properties with the given values",
    "normMissing": {
        "spatial": "Other",
        "topic": "Various other issues",
        "hasWorkflowState": "published",
        "title": "Title n/a",
        "creator": "European Environment Agency (EEA)",
        "organisation": "European Environment Agency (EEA)",
        "format": "text/html",
        "places": "unknown",
        "readingTime": -1,
        "fleschReadingEaseScore": 0,
        "references": [],
        "items_count_references": 1,
        "creation_date": "field:created",
        "issued": "field:creation_date",
        "language": "en",
    },
}
