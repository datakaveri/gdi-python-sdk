import ast
import json
import os
import re

from docstring_parser import parse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

OUTPUT_JSON_PATH = os.path.join(BASE_DIR, "generated-info.json")
DIRECTORY_TO_SCAN = PROJECT_ROOT

# Directories to skip during scanning
SKIP_DIRS = {
    ".venv",
    "venv",
    ".git",
    "__pycache__",
    "node_modules",
    ".uv",
    "node-info-generator",
}

REACTFLOW_PATTERN = re.compile(
    r"""
    ^(?P<base_type>.+?)\s+
    \(
        Reactflow\ will\ (?P<source>
            translate\ it\ as\ input|
            ignore\ this\ parameter|
            take\ it\ from\ the\ previous\ step
        )
        (?:
            ,\s+This\ parameter\ will\ be\ (?P<optional>optional|optoinal)
        )?
    \)$
    """,
    re.VERBOSE,
)


def extract_name(text):
    match = re.search(r"In editor it will be renamed as (\S+)", text)
    if match:
        return match.group(1).rstrip(".,")
    return None


def remove_rename_pattern(text):
    return re.sub(r"In editor it will be renamed as \S+[.,]?", "", text)


def is_supported_reactflow_type(type_name):
    if not isinstance(type_name, str):
        return False
    return REACTFLOW_PATTERN.match(type_name.strip()) is not None


def collect_node_info(directory_to_scan):
    result = []

    for root, dirs, files in os.walk(directory_to_scan):
        # Skip excluded directories
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]

        for filename in files:
            if not filename.endswith(".py"):
                continue

            filepath = os.path.join(root, filename)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    code = f.read()

                feature_type = "raster"
                if "vector_features" in filepath:
                    feature_type = "vector"

                tree = ast.parse(code)
                for node in ast.walk(tree):
                    if not isinstance(node, ast.FunctionDef):
                        continue

                    docstring = ast.get_docstring(node)
                    if not docstring:
                        continue

                    docs = parse(docstring)
                    if len(docs.params) == 0:
                        continue

                    name = extract_name(docs.description) or node.name
                    description = remove_rename_pattern(docs.description)

                    for param in docs.params:
                        if not is_supported_reactflow_type(param.type_name):
                            raise Exception(
                                f"error with function {name} and with `{param.type_name}`"
                            )

                    parameters = {
                        param.arg_name: param.type_name for param in docs.params
                    }
                    result.append(
                        {
                            "nodeName": name,
                            "description": description,
                            "inputs": parameters,
                            "featureType": feature_type,
                        }
                    )

            except (SyntaxError, UnicodeDecodeError):
                continue

    return result


def main():
    result = collect_node_info(DIRECTORY_TO_SCAN)
    os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)

    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"Output saved to {OUTPUT_JSON_PATH}")


if __name__ == "__main__":
    main()
