import logging
import tempfile
from temporalio import activity
from dotenv import load_dotenv

from azure_storage import download_files_from_urls, upload_to_azure_storage

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@activity.defn(name="convert_hdf_to_geotiff")
async def convert_hdf_to_geotiff(hdf_file_url: str, required_dataset="Fpar_500m") -> str:
    import os
    from osgeo import gdal
    from pyhdf.SD import SD, SDC
    import numpy as np

    load_dotenv()

    hdf_file = download_files_from_urls([hdf_file_url])
    hdf = SD(hdf_file, SDC.READ)

    datasets = hdf.datasets().keys()
    logger.info(f"Got datasets {datasets}")

    # Check if the required dataset exists in the HDF file
    if required_dataset not in datasets:
        available_datasets = ", ".join(datasets)
        error_msg = f"Required dataset '{required_dataset}' not found. Available datasets: {available_datasets}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Select the dataset
    selected_dataset = hdf.select(required_dataset)
    data = selected_dataset.get()

    # Get attributes for georeference information
    attributes = selected_dataset.attributes()

    # Create temporary file for GeoTIFF output
    fd, temp_geotiff = tempfile.mkstemp(suffix='.tif')
    os.close(fd)

    # Get spatial metadata from the global attributes if available
    global_attributes = hdf.attributes()

    # Extract geotransform parameters - this is a simplified example
    # Actual parameters should be extracted from metadata
    # For MODIS data, these might be in the StructMetadata.0 attribute

    # Try to extract geolocation information from metadata
    geo_transform = None
    projection = None

    # For MODIS data, we can use the corner coordinates and resolution
    if 'StructMetadata.0' in global_attributes:
        metadata_str = global_attributes['StructMetadata.0']

        # Parse the metadata string to extract the geotransform
        # This is a simplified approach - actual parsing depends on the specific format
        import re

        # Extract upper left corner coordinates
        ul_regex = r'UpperLeftPointMtrs=\(([-+]?\d*\.\d+|\d+),([-+]?\d*\.\d+|\d+)\)'
        ul_match = re.search(ul_regex, metadata_str)

        # Extract lower right corner coordinates
        lr_regex = r'LowerRightMtrs=\(([-+]?\d*\.\d+|\d+),([-+]?\d*\.\d+|\d+)\)'
        lr_match = re.search(lr_regex, metadata_str)

        if ul_match and lr_match:
            ul_x, ul_y = float(ul_match.group(1)), float(ul_match.group(2))
            lr_x, lr_y = float(lr_match.group(1)), float(lr_match.group(2))

            # Get dimensions
            x_dim, y_dim = data.shape

            # Calculate pixel size
            pixel_width = (lr_x - ul_x) / x_dim
            pixel_height = (ul_y - lr_y) / y_dim

            # Create geotransform: (ulx, pixel_width, 0, uly, 0, -pixel_height)
            geo_transform = (ul_x, pixel_width, 0, ul_y, 0, -pixel_height)

            # Set projection - typically MODIS uses sinusoidal projection
            projection = 'PROJCS["Sinusoidal",GEOGCS["GCS_WGS_1984",DATUM["WGS_1984",SPHEROID["WGS_84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Sinusoidal"],PARAMETER["longitude_of_center",0],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["Meter",1]]'

    # If we couldn't extract geotransform, use a default one (this is just a fallback)
    if not geo_transform:
        logger.warning("Could not extract proper geotransform from metadata. Using default values.")
        geo_transform = (-180.0, 0.005, 0.0, 90.0, 0.0, -0.005)  # Default global extent
        projection = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'

    # Replace fill values with NaN
    if '_FillValue' in attributes:
        fill_value = attributes['_FillValue']
        data = np.where(data == fill_value, np.nan, data)

    # Apply scale factor if present
    if 'scale_factor' in attributes:
        scale_factor = attributes['scale_factor']
        data = data * scale_factor

    # Apply offset if present
    if 'add_offset' in attributes:
        add_offset = attributes['add_offset']
        data = data + add_offset

    # Create the GeoTIFF file
    driver = gdal.GetDriverByName('GTiff')
    rows, cols = data.shape

    # Create the output dataset
    dst_ds = driver.Create(temp_geotiff, cols, rows, 1, gdal.GDT_Float32)

    # Set geotransform and projection
    dst_ds.SetGeoTransform(geo_transform)
    dst_ds.SetProjection(projection)

    # Set nodata value
    band = dst_ds.GetRasterBand(1)
    band.SetNoDataValue(np.nan)

    # Write data to the band
    band.WriteArray(data)

    # Flush data to disk
    band.FlushCache()
    dst_ds = None  # Close the dataset

    logger.info(f"Successfully converted HDF to GeoTIFF: {temp_geotiff}")

    geotiff_urls = upload_to_azure_storage('fapar', temp_geotiff)
    return geotiff_urls[0]

if __name__ == '__main__':
    convert_hdf_to_geotiff('https://spmfieldyieldestimation.blob.core.windows.net/fapar/MCD15A2H.A2024361.h25v06.061.2025004042727.hdf')
