import sys
import tempfile
import zipfile
import logging
from typing import List
from temporalio import activity

from pathlib import Path
from dotenv import load_dotenv

from azure_storage import download_files_from_urls, upload_to_azure_storage
from utils import validate_date_format

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@activity.defn(name="download_fapar_data")
async def download_fapar_data(start_date: str, end_date: str, shape_file_url: str) -> str:
    import geopandas as gpd
    import earthaccess

    """Main function."""
    try:
        load_dotenv()
        # Authenticate with NASA Earthdata
        earthaccess.login()

        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract shapefile
            shape_file = download_files_from_urls([shape_file_url])
            shapefile_path = extract_shapefile(shape_file, temp_dir)

            # Read shapefile
            shape_file = gpd.read_file(shapefile_path)

            # Get bounding box
            bbox = get_bounding_box(shape_file)

            # Find data
            granules = find_fapar_data(start_date, end_date, bbox)

            if not granules:
                logger.error("No data granules found for the specified criteria")
                raise ValueError("No data granules found for the specified criteria")

            # Download and process data
            output_files = earthaccess.download(granules, temp_dir)
            logger.info(f"Downloaded to {temp_dir}")

            fapar_urls = upload_to_azure_storage('fapar', output_files[0])
            logger.info("Processing complete.")

            return fapar_urls[0]

    except Exception as e:
        logger.error(f"Error: {e}")
        raise e


def extract_shapefile(zip_path: str, temp_dir: str) -> str:
    """Extract shapefile from zip archive to a temporary directory."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        # Find the .shp file in the extracted content
        shp_files = list(Path(temp_dir).glob('**/*.shp'))
        if not shp_files:
            logger.error("No shapefile found in the zip archive.")
            sys.exit(1)

        return str(shp_files[0])
    except Exception as e:
        logger.error(f"Error extracting shapefile: {e}")
        raise e


def get_bounding_box(gdf):
    """Get the bounding box of the shapefile."""
    # Ensure the GeoDataFrame is in a geographic CRS (WGS84)
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs('EPSG:4326')

    # Get the total bounds of the GeoDataFrame
    minx, miny, maxx, maxy = gdf.total_bounds
    logger.info(f"Bounding box: {minx}, {miny}, {maxx}, {maxy}")

    return minx, miny, maxx, maxy


def find_fapar_data(start_date: str, end_date: str, bbox):
    import earthaccess

    """Find FAPAR data for the given bounding box and date range."""
    # Validate date formats as required by the API
    validate_date_format(start_date, '%Y-%m-%d')
    validate_date_format(end_date, '%Y-%m-%d')

    # Define spatial bounds
    minx, miny, maxx, maxy = bbox

    # Search parameters
    logger.info(f"Searching for FAPAR data between {start_date} and {end_date}")

    # MODIS FAPAR collection short names
    # MOD15A2H is the Terra MODIS LAI/FPAR 8-Day L4 Global 500m product
    # MCD15A2H is the combined Terra and Aqua MODIS LAI/FPAR 8-Day L4 Global 500m product
    # Prefer the combined product when available
    collection_short_name = "MCD15A2H"

    try:
        # Search for granules
        results = earthaccess.search_data(
            short_name=collection_short_name,
            temporal=(start_date, end_date),
            bounding_box=(minx, miny, maxx, maxy),
            count=100  # Adjust as needed
        )

        logger.info(f"Found {len(results)} data granules.")
        return results
    except Exception as e:
        logger.error(f"Error searching for data: {e}")
        # Try alternate collection if first search fails
        try:
            logger.info("Trying alternate FAPAR collection (MOD15A2H)")
            results = earthaccess.search_data(
                short_name="MOD15A2H",
                temporal=(start_date, end_date),
                bounding_box=(minx, miny, maxx, maxy),
                count=100
            )
            logger.info(f"Found {len(results)} data granules from alternate collection.")
            return results
        except Exception as e2:
            logger.error(f"Error searching for alternate data: {e2}")
            return []


if __name__ == '__main__':
    download_fapar_data(
        '2025-01-01',
        '2025-01-08',
        'https://spmfieldyieldestimation.blob.core.windows.net/fapar/East-Nimar-Khandwa.zip')
