# GDI
## 🛠️ Requirements

* Python >= 3.12

## Install poetry and poetry shell
```sh
pip install poetry==2.0.1
```
```sh
poetry self add poetry-plugin-shell
```

## Make an env 
```sh
poetry shell
```
## Install the dependencies
```sh
poetry install
```


---

## 🔐 Authentication

```bash
gdi generate-token --client-id <client-id> --client-secret <client-secret> --role <role>
````

---

## 📍 Vector Commands

### Get Vector Data

**Without Saving:**

```bash
gdi get_vector_data --client-id <client-id> --client-secret <client-secret> --role <role> --resource-id <resource-id>
```

**With Saving:**

```bash
gdi get_vector_data --client-id <client-id> --client-secret <client-secret> --role <role> --resource-id <resource-id> --save-object True --config-path <config-path> --file-path <file-path>
```

### Features Count

```bash
gdi features-count --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url>
```

### List Objects

```bash
gdi ls-objects --config-path <config-path> --client-id <client-id>
```

### Create Buffer

```bash
gdi create-buffer --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url> --buffer-d <buffer-distance> --store-artifact True --file-path <file-path>
```

### Create Intersection

```bash
gdi create-intersection --config-path <config-path> --client-id <client-id> --left_feature <left-feature-path> --right_feature <right-feature-path> --store-artifact True --file-path <file-path>
```

### Download Vector Features

```bash
gdi download_vector_features --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url> --save-as <output-file-path>
```

### List Vector Data

```bash
gdi list_vector_data --location <location>
```

### Compute Geometry

```bash
gdi compute-geometry --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url> --store-artifact True --file-path <file-path>
```

### Reduce to Raster

```bash
gdi reduce_to_raster --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url> --attribute <attribute> --grid-size <grid-size> --reducer <reducer> --store-artifacts <True/False> --file-path <file-path>
```

### Create Optimal Route

```bash
gdi create-optimal-route --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url> --points-filepath <points-filepath> --store-artifacts True --route-file-path <route-file-path>
```

### Create Voronoi

```bash
gdi create-voronoi --config-path <config-path> --client-id <client-id> --input-artifact-url <artifact-url> --store-artifact True --file-path <file-path>
```

### Delaunay Triangulation

```bash
gdi create-delaunay-triangles --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url> --store-artifact <storage-location> --file-path <file-path>
```

### Clip Vector

```bash
gdi clip-vector --config-path <config-path> --client-id <client-id> --target-artifact-url <target-artifact-url> --clip-artifact-url <clip-artifact-url> --store-artifact <storage-location> --file-path <file-path>
```

### BBOX Vector Clip

```bash
gdi bbox-feature-clip --config-path <config-path> --client-id <client-id> --target-artifact-url <target-artifact-url> --clip-vector-path <clip-vector-path> --store-artifact <storage-location> --file-path <file-path>
```

---

## 🗺️ Raster Commands

### Search Catalog

```bash
gdi search-cat --collection-ids <collection-id>
```

### Get STAC Assets

```bash
gdi get-raster-data --client-id <client-id> --client-secret <client-secret> --role <role> --collection-ds <collection-ds> --config-path <config-path>
```

### Flood Fill Model

```bash
gdi flood-fill-model --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url> --threshold <threshold> --store-artifact True --file-path <file-path>
```

### Generate NDVI

```bash
gdi generate-ndvi --config-path <config-path> --client-id <client-id> --red-artifact-url <red-artifact-url> --nir-artifact-url <nir-artifact-url> --store-artifact <storage-location> --file-path <file-path>
```

### Compute Slope

```bash
gdi generate-slope --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url> --store-artifact <storage-location> --file-path <file-path>
```

### Generate Isometric Lines

```bash
gdi generate-isometric-lines --config-path <config-path> --client-id <client-id> --artifact-url <artifact-url> --interval <interval> --store-artifact <storage-location> --file-path <file-path>
```

### Reduce to Feature

```bash
gdi reduce-to-feature --config-path <config-path> --client-id <client-id> --raster-artifact-url <raster-artifact-url> --vector-artifact-url <vector-artifact-url> --reducer <reducer> --attribute <attribute> --store-artifact <storage-location> --file-path <file-path>
```

### Merge Rasters

```bash
gdi rasters-merge --config-path <config-path> --client-id <client-id> --prefix <prefix> --store-artifact <storage-location> --file-path <file-path>
```

### Clip Raster

```bash
gdi raster-clip --config-path <config-path> --client-id <client-id> --raster-key <raster-key> --geojson-key <geojson-key> --store-artifact <storage-location> --file-path <file-path>
```

### BBOX Raster Clip

```bash
gdi bbox-raster-clip --config-path <config-path> --client-id <client-id> --raster-key <raster-key> --vector-path <vector-path> --store-artifact <storage-location> --file-path <file-path>
```

### Generate Local Correlation

```bash
gdi generate-local-correlation --config-path <config-path> --client-id <client-id> --x <band_path> --y <band_path> --chunk-size <chunk_size> --store-artifact <storage-location> --file-path <file-path>
```

### Extract Band Path

```bash
gdi extract-band-path --asset-list <asset-list> --item-key <item-key> --asset-key <asset-key>
```

---



## For Reference : 
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

---
## 📄 Notes

* Make sure your `config-path` file is correctly set up with MinIO credentials and bucket info.
* `store-artifact` and `store-artifacts` must be explicitly set to `True` or a valid storage destination.
* `<artifact-url>` and `<file-path>` must be adjusted to reflect your environment and bucket layout.

