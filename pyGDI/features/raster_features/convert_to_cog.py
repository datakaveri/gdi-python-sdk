from osgeo import gdal

def tiff_to_cogtiff(input_tif:str,cog_tiff:str):
              
    # Convert to COG using GDAL               
    translate_options = gdal.TranslateOptions(format="COG", metadataOptions=["COPY_SRC_OVERVIEWS=YES"])
    gdal.Translate(cog_tiff, input_tif, options=translate_options)
    
    return cog_tiff
