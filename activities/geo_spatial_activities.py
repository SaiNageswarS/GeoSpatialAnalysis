import logging
from pathlib import Path

from temporalio import activity
from azure_storage import AzureStorage

from activities.download_mosdac_data import download_mosdac_data
from activities.scale_tiff import scale_tiff
from activities.compose_tiff import compose_tiff
from activities.download_fapar_data import download_fapar_data
from activities.convert_hdf_to_geotiff import convert_hdf_to_geotiff

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GeoSpatialActivities:
    def __init__(self, config: dict[str, str], azure_storage: AzureStorage):
        self._config = config
        self._azure_storage = azure_storage

    @activity.defn(name="download_mosdac_data")
    async def download_mosdac_data(self, remote_path: str) -> list[str]:
        info = activity.info()
        workflow_id = info.workflow_id

        files = download_mosdac_data(remote_path)
        for file in files:
            self._azure_storage.upload_bytes(f"{workflow_id}/{file.name}", read_file_to_bytes(file))

        return [x.name for x in files]

    @activity.defn(name="scale_tiff")
    async def scale_tif(self, tif_file_name: str, scale_factor=0.5) -> str:
        info = activity.info()
        workflow_id = info.workflow_id

        tif_file = self._azure_storage.download_file(f"{workflow_id}/{tif_file_name}")
        scaled_tif_file = scale_tiff(tif_file, scale_factor)
        self._azure_storage.upload_bytes(f"{workflow_id}/{scaled_tif_file.name}", read_file_to_bytes(scaled_tif_file))

        return scaled_tif_file.name

    @activity.defn(name="compose_tiffs")
    async def compose_tifs(self, tif_files: list[str]) -> str:
        info = activity.info()
        workflow_id = info.workflow_id

        input_tiffs = []
        for tif_file in tif_files:
            downloaded_file = self._azure_storage.download_file(f"{workflow_id}/{tif_file}")
            input_tiffs.append(downloaded_file)

        composed_tif = compose_tiff(input_tiffs)
        self._azure_storage.upload_bytes(f"{workflow_id}/{composed_tif.name}", read_file_to_bytes(composed_tif))

        return composed_tif.name

    @activity.defn(name="download_fapar_data")
    async def download_fapar_data(self, start_date: str, end_date: str, shape_file_name: str) -> str:
        info = activity.info()
        workflow_id = info.workflow_id

        shape_file = self._azure_storage.download_file(shape_file_name)
        fapar_hdf_path = download_fapar_data(start_date, end_date, shape_file)

        fapar_hdf = Path(fapar_hdf_path)
        self._azure_storage.upload_bytes(f"{workflow_id}/{fapar_hdf.name}", read_file_to_bytes(fapar_hdf))

        return fapar_hdf.name

    @activity.defn(name="convert_hdf_to_geotiff")
    async def convert_hdf_to_geotiff(self, hdf_file_name, required_dataset="Fpar_500m"):
        info = activity.info()
        workflow_id = info.workflow_id

        hdf_file = self._azure_storage.download_file(f"{workflow_id}/{hdf_file_name}")
        geotif_file = convert_hdf_to_geotiff(hdf_file, required_dataset)

        self._azure_storage.upload_bytes(f"{workflow_id}/{geotif_file.name}", read_file_to_bytes(geotif_file))
        return geotif_file.name


def read_file_to_bytes(file_path: Path):
    """Reads a file and returns its content as bytes."""
    with open(file_path, 'rb') as file:
        file_bytes = file.read()
    return file_bytes
