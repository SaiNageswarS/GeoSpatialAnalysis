from temporalio import activity


@activity.defn(name="compose_tiffs")
async def compose_tiff(input_root: str) -> str:
    import os
    import tempfile
    from osgeo import gdal

    output_tiff = os.path.join(input_root, "composed_output.tif")

    """Composes multiple TIFF files from different folders into a single output TIFF."""
    input_tiffs = []

    print(f"Reading input from {input_root}")
    for root, _, files in os.walk(input_root):
        for file in files:
            if file.lower().endswith(".tif") or file.lower().endswith(".tiff"):
                input_tiffs.append(os.path.join(root, file))

    if not input_tiffs:
        print("No TIFF files found for composition.")
        return ""

    vrt_options = gdal.BuildVRTOptions(resampleAlg='nearest')
    vrt_path = tempfile.NamedTemporaryFile(suffix=".vrt", delete=False).name
    gdal.BuildVRT(vrt_path, input_tiffs, options=vrt_options)

    gdal.Translate(output_tiff, vrt_path, format="GTiff")
    print(f"Composed TIFF saved to {output_tiff}")

    return output_tiff
