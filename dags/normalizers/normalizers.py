def simplify_elements(element, element_key):
    clean_element = {}
    if isinstance(element, dict):
        for key in element.keys():
            new_elements = simplify_elements(element[key], key)
            for new_key in new_elements.keys():

                new_element_key = new_key
                if len(element_key) > 0:
                    new_element_key = element_key + "." + new_key
                clean_element[new_element_key] = new_elements[new_key]
    else:
      clean_element[element_key] = element
    return clean_element

def create_doc(doc):
    return simplify_elements(doc, '')

def apply_black_map(doc, config):
    black_map = config['blackMap']
    clean_data = {}
    for key in doc.keys():
        value = None
        if black_map.get(key, None) is None:
            value = doc[key]
        else:
            if isinstance(doc[key], list):
                tmp_value = []
                for val in doc[key]:
                    if val not in black_map[key]:
                        tmp_value.append(val)
                value = tmp_value
            else:
                if doc[key] in black_map[key]:
                    value = None
        clean_data[key] = value
    return clean_data

def apply_white_map(doc, config):
    white_map = config['whiteMap']
    clean_data = {}
    for key in doc.keys():
        value = None
        if white_map.get(key, None) is None:
            value = doc[key]
        else:
            if isinstance(doc[key], list):
                tmp_value = []
                for val in doc[key]:
                    if val in white_map[key]:
                        tmp_value.append(val)
                value = tmp_value
            else:
                if doc[key] in white_map[key]:
                    value = doc[key]
        clean_data[key] = value
    return clean_data

def remove_empty(doc):
    clean_data = {}
    for key in doc.keys():
        ignore_attr = False
        if isinstance(doc[key], list):
            if len(doc[key]) == 0:
                ignore_attr = True
        else:
            if doc[key] is None:
                ignore_attr = True
            else:
                 if isinstance(doc[key], str) and len(doc[key]) == 0:
                    ignore_attr = True
        if not ignore_attr:
            clean_data[key] = doc[key]
    return clean_data

def apply_norm_obj(doc, config):
    norm_obj = config['normObj']
    clean_data = {}
    for key in doc.keys():
        value = doc[key]
        if isinstance(doc[key], list):
            value = []
            for val in doc[key]:
                if norm_obj.get(val, None) != None:
                    value.append(norm_obj[val])
                else:
                    value.append(val)
        else:
            if norm_obj.get(value, None) != None:
                value = norm_obj[value]
        clean_data[key] = value
    return clean_data

def apply_norm_prop(doc, config):
    norm_prop = config['normProp']
    clean_data = {}
    for key in doc.keys():
        value = doc[key]
        if norm_prop.get(key, None) == None:
            clean_data[key] = value
        else:
            if (not isinstance(norm_prop[key], list)):
                norm_prop[key] = [norm_prop[key]]
            for new_key in norm_prop[key]:
                clean_data[new_key] = value
    return clean_data

def apply_norm_missing(doc, config):
    norm_missing = config['normMissing']
    clean_data = doc
    for key in norm_missing.keys():
        if clean_data.get(key, None) == None:
            clean_data[key] = norm_missing[key]
    return clean_data

def remove_duplicates(doc):
    clean_data = {}
    for key in doc.keys():
        value = doc[key]
        if isinstance(value, list):
            value = list(dict.fromkeys(value))
        clean_data[key] = value
    return clean_data
