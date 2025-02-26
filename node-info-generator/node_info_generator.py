import ast
import os
import json
from docstring_parser import parse
FUNCTION_JSON_PATH = '/home/pradeep/gdi-python-sdk2/node-info-generator/functions.json'
OUTPUT_JSON_PATH = '/home/pradeep/gdi-python-sdk2/node-info-generator/generated-info.json'
DIRECTORY_TO_SCAN = '/home/pradeep/gdi-python-sdk2'
try:
    with open(FUNCTION_JSON_PATH) as f:
        function_config = json.load(f)
        function_map = {}
        for item in function_config:
            function_map[item["name"]] = item["rename"]
except Exception as e:
    print(f"Failed to load function config: {e}")
    exit(1)
result = []
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
                    if original_name in function_map:
                        new_name = function_map[original_name]
                        docstring = ast.get_docstring(node)
                        parameters = []
                        if docstring:
                            docs = parse(docstring)
                            parameters={param.arg_name: param.type_name for param in docs.params}
                        result.append({
                            "nodeName": new_name,
                            "inputs": parameters
                        })
        except (SyntaxError, UnicodeDecodeError):
            continue

with open(OUTPUT_JSON_PATH, "w") as f:
    json.dump(result, f, indent=2)

print(f"Output saved to {OUTPUT_JSON_PATH}")
