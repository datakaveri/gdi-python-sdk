# Vector Commands

## Fetching Resources

### Fetch resource without saving it
```sh
gdi fetch-resource --client-id <client-id> --client-secret <client-secret> --role consumer --resource-id <resource-id>
```

### Fetch resource and save it
```sh
gdi fetch-resource --client-id <client-id> --client-secret <client-secret> --role consumer --resource-id <resource-id> --save-object True --config-path <config-path> --file-path <file-path>
```

## Authentication

### Get an authentication token
```sh
gdi generate-token --client-id <client-id> --client-secret <client-secret> --role consumer
```

## Vector Operations

### Get the number of features in the vectors
```sh
gdi features-count --config <config-path> --client-id <client-id> --artefact-url <artifact-url>
```

### List objects in MinIO storage
```sh
gdi ls-objects --config <config-path> --client-id <client-id>
```

### Create buffer for vectors
```sh
gdi create-buffer --config <config-path> --client-id <client-id> --artifact-url <artifact-url> --buffer-d <buffer-distance> --store-artifact True --file-path <file-path>
```

### Intersect two buffered datasets
```sh
gdi create-intersection --config <config-path> --client-id <client-id> --left_feature <left-feature-path> --right_feature <right-feature-path> --store-artifact True --file-path <file-path>
```

### Download an artifact
```sh
gdi download-artifact --config <config-path> --client-id <client-id> --artifact-url <artifact-url> --save-as <output-file-path>
```

### Search for data
```sh
gdi list-data --location <location>
```

### Compute geometry
```sh
gdi compute-geometry --config <config-path> --client-id <client-id> --artifact-url <artifact-url> --store-artifact True --file-path <file-path>
```

### Reduce to image
```sh
gdi reduce-to-img --config <config-path> --client-id <client-id> --artifact-url <artifact-url> --attribute <attribute> --grid-size <grid-size> --reducer <reducer> --store-artifacts <True/False> --file-path <file-path>
```

### Create optimal route given points and vectors
```sh
gdi create-optimal-route --config <config-path> --client-id <client-id> --artifact-url <artifact-url> --points-filepath <points-filepath> --store-artifacts True --route-file-path <route-file-path>
```

## Raster Function Utilities

### Search a collection
```sh
gdi search-cat --collection-ids <collection-id>
```

### Get STAC data
```sh
gdi get-stac-assets --client-id <client-id> --client-secret <client-secret> --role consumer --collection-ds <collection-ds> --config <config-path>
```

## FOR REFERENCE : 
```
client-id  = 7dcf1193-4237-48a7-a5f2-4b530b69b1cb
```
```
client secret = a863cafce5bd3d1bd302ab079242790d18cec974
```
## To know more about any of the commands:
```
gdi <command-name> -- help
```
