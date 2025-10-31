from osgeo import gdal
import warnings
warnings.filterwarnings("ignore")

def tiff_to_cogtiff(input_tif:str,cog_tiff:str):
              
    # Convert to COG using GDAL               
    translate_options = gdal.TranslateOptions(format="COG", metadataOptions=["COPY_SRC_OVERVIEWS=YES"])
    gdal.Translate(cog_tiff, input_tif, options=translate_options)

def tiff_to_cogtiff_v2(input_tif: str, cog_tiff: str):
    """
    Convert a regular GeoTIFF to a valid Cloud-Optimized GeoTIFF (COG)
    with compression and proper tiling.
    """
    translate_options = gdal.TranslateOptions(
        format="COG",
        creationOptions=[
            "COMPRESS=LZW",
            "BLOCKSIZE=512",
            "OVERVIEW_RESAMPLING=AVERAGE",
            "NUM_THREADS=ALL_CPUS"
        ]
    )
    gdal.Translate(cog_tiff, input_tif, options=translate_options)



    
