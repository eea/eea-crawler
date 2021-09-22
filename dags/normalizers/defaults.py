normalizers = {
    "//": "Properties that are indexed into ElasticSearch",
    "proplist":[
        "language",
        "title",
        "description",
        "themes",
        "subject",
        "location"
    ],
    "//": "Normalise Properties, when you want to use a property instead of another",
    "normProp":{
        "temporalCoverage":["time_coverage"],
        "title":["title","label"],
        "expirationDate":"expires"
    },
    "//": "Do not index these values for a given properties",
    "blackMap": {
        "expires": ["None", "Unknown"]
    },
    "//": "Index _only_ these values for given properties",
    "whiteMap": {
    },
    "//": "Normalise these following objects to a given value",
    "normObj": {
        "Czech Republic" : "Czechia"
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
      "places" : "unknown",
      "readingTime" : -1,
      "fleschReadingEaseScore" : 0,
      "references":[]
    }
}
