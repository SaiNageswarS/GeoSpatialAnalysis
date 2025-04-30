import os
import asyncio
from dotenv import load_dotenv
import logging

from temporalio.worker import Worker
from utils import connect_with_backoff
from workflows.fapar import ProcessFapar
from workflows.mosdac import ProcessMosdac

from activities.compose_tiff import compose_tiff
from activities.download_mosdac_data import download_mosdac_data
from activities.scale_tiff import scale_tiff
from activities.download_fapar_data import download_fapar_data

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def main():
    load_dotenv()

    temporal_host = os.getenv("TEMPORAL_SERVER")

    client = await connect_with_backoff(temporal_host)
    worker = Worker(
        client,
        task_queue="GeoSpatialAnalysisQueue",
        workflows=[ProcessMosdac, ProcessFapar],
        activities=[download_mosdac_data, scale_tiff, compose_tiff, download_fapar_data],
    )

    logger.info("🚀 Starting Temporal Worker...")
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())
