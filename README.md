# GDI COMMANDS

## Overview
This document provides a list of commands for interacting with GDI, including fetching resources, generating authentication tokens, and working with MinIO objects.

## Commands

### 1. Fetch Resource Without Saving
Use this command to fetch a resource without saving it to a file:
```sh
gdi fetch-resource --client-id 7dcf1193-4237-48a7-a5f2-4b530b69b1cb --client-secret a863cafce5bd3d1bd302ab079242790d18cec974 --role consumer --resource-id 024b0c51-e44d-424c-926e-254b6c966978
```

### 2. Fetch Resource and Save It
Use this command to fetch a resource and save it to a specified file:
```sh
gdi fetch-resource --client-id 7dcf1193-4237-48a7-a5f2-4b530b69b1cb --client-secret a863cafce5bd3d1bd302ab079242790d18cec974 --role consumer --resource-id 024b0c51-e44d-424c-926e-254b6c966978 --save-object True --config-path config.json --file-path data_new1.gpkg
```

### 3. Generate Authentication Token
Use this command to generate an authentication token:
```sh
gdi generate-token --client-id 7dcf1193-4237-48a7-a5f2-4b530b69b1cb --client-secret a863cafce5bd3d1bd302ab079242790d18cec974 --role consumer
```

### 4. Get the Number of Features in Vectors
Use this command to get the number of features in a vector file:
```sh
gdi features-count --config config.json --client-id 7dcf1193-4237-48a7-a5f2-4b530b69b1cb --artefact-url data_new1.gpkg
```

### 5. List Objects in MinIO
Use this command to list objects in MinIO storage:
```sh
gdi ls-objects --config config.json --client-id 7dcf1193-4237-48a7-a5f2-4b530b69b1cb
```

### 6. Download features
Use this command to download file to MinIO storage with user specified gpkg:
```sh
gdi download-artefact --config config.json --client-id 7dcf1193-4237-48a7-a5f2-4b530b69b1cb --artefact-url data_new1.pkl --save-as inter/data_new1.gpkg
```

### 7. create buffer
Use this command to create buffer for the featutre vectors
```sh
gdi create-buffer --config config.json --client-id 7dcf1193-4237-48a7-a5f2-4b530b69b1cb --artifact-url 1b2a07b7-f423-4dd3-bdee-9a6af6fe47f9.pkl --buffer-d 0.9  --store-artifact True --file-path buffer_item/data_1.pkl
```



## Notes
- Ensure that `gdi` is installed and configured properly before running these commands.
- Replace `client-id`, `client-secret`, and `resource-id` with appropriate values if required.
- The `config.json` file should be properly configured for commands that require it.
- The `data_new1.gpkg` file is an example file name; change it accordingly based on your requirements.



