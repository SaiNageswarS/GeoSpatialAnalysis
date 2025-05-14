from temporalio import workflow
import logging
from datetime import timedelta

from activities.download_fapar_data import download_fapar_data
from activities.convert_hdf_to_geotiff import convert_hdf_to_geotiff
from activities.scale_tiff import scale_tiff

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@workflow.defn(name="ProcessFapar")
class ProcessFapar:
    @workflow.run
    async def run(self, args) -> str:
        logger.info(f"🚨 Workflow Args: {args} ({type(args)})")

        fapar_data = await workflow.execute_activity(
            download_fapar_data,
            args=[args["start_date"], args["end_date"], args["shape_file_url"]],
            start_to_close_timeout=timedelta(seconds=300),
        )

        geotif_url = await workflow.execute_activity(
            convert_hdf_to_geotiff,
            args=[fapar_data],
            start_to_close_timeout=timedelta(seconds=300),
        )

        rescaled_tif = await workflow.execute_activity(
            scale_tiff,
            args=[geotif_url, "fapar"],
            start_to_close_timeout=timedelta(seconds=300),
        )

        return rescaled_tif
