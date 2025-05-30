import ast
import os
import json
import re 
from docstring_parser import parse
OUTPUT_JSON_PATH = '/home/pradeep/gdi/gdi-python-sdk/node-info-generator/generated-info.json'
DIRECTORY_TO_SCAN = '/home/pradeep/gdi/gdi-python-sdk/'
result = []
allAttribute=["(Reactflow will translate it as input)","(Reactflow will ignore this parameter)","(Reactflow will take it from the previous step)"]

def extract_name(text):
    match = re.search(r"In editor it will be renamed as (\S+)", text)
    if match:
        return match.group(1).rstrip(".,")
    return None


def remove_rename_pattern(text):
    return re.sub(r"In editor it will be renamed as \S+[.,]?", "", text)

def contains_any_string(target_string, string_array):
    return any(s in target_string for s in string_array)
for root, dirs, files in os.walk(DIRECTORY_TO_SCAN):
    for filename in files:
        if not filename.endswith(".py"):
            continue
        filepath = os.path.join(root, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                code = f.read()
            featureType="raster"
            if "vector_features" in filepath:
                featureType ="vector"
            tree = ast.parse(code)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    original_name = node.name
                    docstring = ast.get_docstring(node)
                    parameters = []
                    if docstring:
                      docs = parse(docstring)
                      if len(docs.params)==0:
                            continue
                      name = extract_name(docs.description) or node.name
                      description=remove_rename_pattern(docs.description)
                      for param in docs.params:
                        if not contains_any_string(param.type_name, allAttribute):
                          message = f"error with function {name} and with `{param.type_name}`"
                          raise Exception(message)
                      parameters={param.arg_name: param.type_name for param in docs.params}
                      result.append({
                          "nodeName": name,
                          "description": description,
                          "inputs": parameters,
                          "featureType":featureType
                      })
        except (SyntaxError, UnicodeDecodeError):
            continue

with open(OUTPUT_JSON_PATH, "w") as f:
    json.dump(result, f, indent=2)

print(f"Output saved to {OUTPUT_JSON_PATH}")
