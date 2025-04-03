# Update Function Names and Generate Node Info
 
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
 FUNCTION_JSON_PATH = "path/to/your/functions.json"  
 OUTPUT_JSON_PATH = "path/to/output_data.json" 
 ```
 
 ## 3.  Install Required Package
 
 ```sh
 pip install docstring_parser
 ```
 
 ## 4. Run the script
 ```sh
 python ./node_info_generator.py
 ```


## 4. Feature
1. functions which are written with documentation can be transform as nodes 
2. if you write `In editor it will be renamed as (string-with-hyphen-and-without-small-brakcet)` it will rename it as `string-with-hyphen` you can see it in `output_data.json`
