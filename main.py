import os
import asyncio
from dotenv import load_dotenv
import logging
import configparser

from azure_storage import AzureStorage

from temporalio.worker import Worker
from utils import connect_with_backoff
from workflows.fapar import ProcessFapar
from workflows.mosdac import ProcessMosdac

from activities.geo_spatial_activities import GeoSpatialActivities

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():
    load_dotenv()
    run_mode = os.getenv("ENV", "dev").lower()

    config = configparser.ConfigParser()
    config.read('config.ini')
    env_config = dict(config[run_mode])

    temporal_host = env_config["temporal_host_port"]

    azure_storage = AzureStorage(env_config)
    geo_spatial_activities = GeoSpatialActivities(env_config, azure_storage)

    client = await connect_with_backoff(temporal_host)
    worker = Worker(
        client,
        task_queue="GeoSpatialAnalysisQueue",
        workflows=[ProcessMosdac, ProcessFapar],
        activities=[geo_spatial_activities.download_mosdac_data,
                    geo_spatial_activities.scale_tif,
                    geo_spatial_activities.compose_tifs,
                    geo_spatial_activities.download_fapar_data,
                    geo_spatial_activities.convert_hdf_to_geotiff],
    )

    logger.info("🚀 Starting Temporal Worker...")
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())
