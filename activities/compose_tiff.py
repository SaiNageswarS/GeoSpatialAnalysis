from pathlib import Path


def compose_tiff(input_tiffs: list[str]) -> Path:
    import os
    import tempfile
    from osgeo import gdal

    temp_dir = tempfile.mkdtemp(prefix="compose_tif_")
    output_tiff = Path(temp_dir).joinpath("composed_output.tif")

    vrt_options = gdal.BuildVRTOptions(resampleAlg='nearest')
    vrt_path = tempfile.NamedTemporaryFile(suffix=".vrt", delete=False).name
    gdal.BuildVRT(vrt_path, input_tiffs, options=vrt_options)

    gdal.Translate(str(output_tiff), vrt_path, format="GTiff")
    print(f"Composed TIFF saved to {output_tiff}")

    return output_tiff
