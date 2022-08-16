import json

def load_variables_from_disk(fileName):
    f = open(fileName)
    variables = json.load(f)
    f.close()
    return variables
