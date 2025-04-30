import os
import tempfile
from temporalio import activity

from azure_storage import download_files_from_urls, upload_to_azure_storage


@activity.defn(name="scale_tiff")
async def scale_tiff(azure_paths: list[str], scale_factor=0.5) -> list[str]:
    """Recursively finds and scales all TIFF files in the input folder."""
    input_folder = download_files_from_urls(azure_paths)
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
    azure_urls = upload_to_azure_storage("scaled-par", temp_root)
    return azure_urls


def __scale_tiff__(input_tiff: str, output_tiff: str, scale_factor=0.5) -> str:
    from osgeo import gdal
    import numpy as np

    # Open the source TIFF file
    dataset = gdal.Open(input_tiff, gdal.GA_ReadOnly)
    driver = gdal.GetDriverByName('GTiff')

    # Get basic info
    cols = dataset.RasterXSize
    rows = dataset.RasterYSize
    bands = dataset.RasterCount
    geotransform = dataset.GetGeoTransform()
    projection = dataset.GetProjection()

    # Create output GeoTIFF
    out_ds = driver.Create(output_tiff, cols, rows, bands, gdal.GDT_Float32)
    out_ds.SetGeoTransform(geotransform)
    out_ds.SetProjection(projection)

    # Process each band
    for i in range(1, bands + 1):
        band = dataset.GetRasterBand(i)
        data = band.ReadAsArray().astype(np.float32)
        data *= 0.5  # Multiply by 0.5
        out_band = out_ds.GetRasterBand(i)
        out_band.WriteArray(data)
        out_band.SetNoDataValue(band.GetNoDataValue())

    # Cleanup
    dataset = None
    out_ds = None
