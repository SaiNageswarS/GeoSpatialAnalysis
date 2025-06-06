import os
import tempfile
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def scale_tiff(original_tif: str, scale_factor=0.5) -> Path:
    from osgeo import gdal

    logger.info(f"Scaling tiff {original_tif}")

    src_ds = gdal.Open(original_tif, gdal.GA_ReadOnly)
    if src_ds is None:
        error_msg = f"Could not open input file: {original_tif}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Get the input dimensions
    src_width = src_ds.RasterXSize
    src_height = src_ds.RasterYSize

    # Calculate the new dimensions
    dst_width = int(src_width * scale_factor)
    dst_height = int(src_height * scale_factor)

    logger.info(f"Scaling GeoTIFF from {src_width}x{src_height} to {dst_width}x{dst_height}")

    # Get other parameters from the source dataset
    band_count = src_ds.RasterCount
    data_type = src_ds.GetRasterBand(1).DataType

    # Get geotransform and adjust it according to the scale factor
    geotransform = list(src_ds.GetGeoTransform())
    # Scale the pixel width and height (elements 1 and 5)
    geotransform[1] = geotransform[1] / scale_factor  # Pixel width
    geotransform[5] = geotransform[5] / scale_factor  # Pixel height (negative for north-up images)

    # Get projection
    projection = src_ds.GetProjection()

    # Create the output dataset
    driver = gdal.GetDriverByName('GTiff')

    original_tif_file_name = Path(original_tif).name
    temp_dir = tempfile.mkdtemp(prefix="scale_tif_")
    output_tif_path = Path(temp_dir).joinpath(f"scaled_{original_tif_file_name}")

    dst_ds = driver.Create(str(output_tif_path), dst_width, dst_height, band_count, data_type)

    if dst_ds is None:
        error_msg = f"Could not create output file: {output_tif_path}"
        logger.error(error_msg)
        src_ds = None  # Close the dataset
        raise RuntimeError(error_msg)

    # Set the geotransform and projection on the output dataset
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(projection)

    # Set up resampling options
    # Options: 'near', 'bilinear', 'cubic', 'cubicspline', 'lanczos', 'average', 'mode'
    resampling = gdal.GRA_Bilinear  # Default to bilinear for better quality

    # Process each band
    for band_idx in range(1, band_count + 1):
        src_band = src_ds.GetRasterBand(band_idx)
        dst_band = dst_ds.GetRasterBand(band_idx)

        # Copy nodata value if present
        nodata_value = src_band.GetNoDataValue()
        if nodata_value is not None:
            dst_band.SetNoDataValue(nodata_value)

        # Copy color interpretation if present
        color_interp = src_band.GetColorInterpretation()
        dst_band.SetColorInterpretation(color_interp)

        # Copy color table if present
        color_table = src_band.GetColorTable()
        if color_table is not None:
            dst_band.SetColorTable(color_table)

    # Perform the actual resampling
    gdal.ReprojectImage(
        src_ds,           # Source dataset
        dst_ds,           # Destination dataset
        None,             # Source projection (None = use source dataset's projection)
        None,             # Destination projection (None = use destination dataset's projection)
        resampling        # Resampling algorithm
    )

    # Copy metadata
    dst_ds.SetMetadata(src_ds.GetMetadata())

    # Clean up
    dst_ds.FlushCache()  # Write to disk
    dst_ds = None  # Close the dataset
    src_ds = None  # Close the dataset

    logger.info(f"Successfully scaled GeoTIFF to {output_tif_path}")
    return output_tif_path


