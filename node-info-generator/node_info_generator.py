import ast
import os
import json
import re 
from docstring_parser import parse
OUTPUT_JSON_PATH = 'J:/New_folder_2/gdi-python-sdk/node-info-generator/generated-info.json'
DIRECTORY_TO_SCAN = 'J:/New_folder_2/gdi-python-sdk'
result = []
allAttribute=["(Reactflow will translate it as input)","(Reactflow will ignore this parameter)","(Reactflow will take it from the previous step)"]
def extract_name(text):
    match = re.search(r"In editor it will be rename as (\S+)", text)
    if match:
        return match.group(1).rstrip(".,")
    return None
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
                      for param in docs.params:
                        if not contains_any_string(param.type_name, allAttribute):
                          message = f"error with function {name} and with `{param.type_name}`"
                          raise Exception(message)
                      parameters={param.arg_name: param.type_name for param in docs.params}
                      result.append({
                          "nodeName": name,
                          "inputs": parameters
                      })
        except (SyntaxError, UnicodeDecodeError):
            continue

with open(OUTPUT_JSON_PATH, "w") as f:
    json.dump(result, f, indent=2)

print(f"Output saved to {OUTPUT_JSON_PATH}")
