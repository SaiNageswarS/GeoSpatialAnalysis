import os
import tempfile
from temporalio import activity


@activity.defn(name="scale_tiff")
async def scale_tiff(input_folder: str, scale_factor=0.5) -> str:
    """Recursively finds and scales all TIFF files in the input folder."""
    print(f"Scaling tiff in {input_folder}")
    temp_root = tempfile.mkdtemp()
    temp_files = []

    for root, _, files in os.walk(input_folder):
        relative_path = os.path.relpath(root, input_folder)
        temp_subfolder = os.path.join(temp_root, relative_path)
        os.makedirs(temp_subfolder, exist_ok=True)

        for file in files:
            if file.lower().endswith(".tif") or file.lower().endswith(".tiff"):
                input_tiff = os.path.join(root, file)
                output_tiff = os.path.join(temp_subfolder, file)
                print(f"Processing {input_tiff} -> {output_tiff}")
                temp_output = __scale_tiff__(input_tiff, output_tiff, scale_factor)
                if temp_output:
                    temp_files.append(temp_output)

    print(f"All scaled TIFFs stored in {temp_root}")
    return temp_root


def __scale_tiff__(input_tiff: str, output_tiff: str, scale_factor=0.5) -> str:
    from osgeo import gdal  # ✅ Import locally to avoid polluting workflow sandbox

    # Open the source TIFF file
    src_ds = gdal.Open(input_tiff, gdal.GA_ReadOnly)
    if src_ds is None:
        print("Error: Unable to open input TIFF file.")
        return ""

    # Get original dimensions
    width = src_ds.RasterXSize
    height = src_ds.RasterYSize
    bands = src_ds.RasterCount

    # Calculate new dimensions
    new_width = int(width * scale_factor)
    new_height = int(height * scale_factor)

    # Get the driver for GeoTIFF
    driver = gdal.GetDriverByName("GTiff")

    # Create the scaled output dataset
    dst_ds = driver.Create(output_tiff, new_width, new_height, bands, gdal.GDT_Byte)

    # Set geotransform and projection
    dst_ds.SetGeoTransform(src_ds.GetGeoTransform())
    dst_ds.SetProjection(src_ds.GetProjection())

    # Resample each band
    for band in range(1, bands + 1):
        src_band = src_ds.GetRasterBand(band)
        dst_band = dst_ds.GetRasterBand(band)

        # Perform scaling using nearest neighbor interpolation
        gdal.RegenerateOverview(src_band, dst_band, 'nearest')

    # Clean up
    dst_ds = None
    src_ds = None

    print(f"Scaled TIFF saved to {output_tiff}")
    return output_tiff
