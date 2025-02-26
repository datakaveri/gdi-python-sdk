## 1. Update `functions.json`
Add new function names in this format:
```json
{
    "name": "original_function_name",
    "rename": "new_node_name"
}
```

## 2. Set File Paths
```python
FUNCTION_JSON_PATH = "path/to/your/functions.json"  # Path to your functions list
OUTPUT_JSON_PATH = "path/to/output_data.json"       # Where to save extracted data
DIRECTORY_TO_SCAN = "path/to/your/code_folder"     # Folder containing your code files
```

## 3.  Install Required Package

```sh
pip install docstring_parser
```

## 4. Run the script
```sh
python ./node_info_generator.py
```
