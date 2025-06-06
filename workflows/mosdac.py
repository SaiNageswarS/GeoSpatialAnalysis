from temporalio import workflow
from datetime import timedelta
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@workflow.defn(name="ProcessMosdac")
class ProcessMosdac:
    @workflow.run
    async def run(self, args) -> str:
        logger.info(f"🚨 Workflow Args: {args} ({type(args)})")
        wid = workflow.info().workflow_id

        # Step 1: Download the data
        input_folder = await workflow.execute_activity(
            "download_mosdac_data",
            args=[args["remote_path"]],
            start_to_close_timeout=timedelta(seconds=3000),
        )

        # Step 2: Scale the TIFF files
        scaled_urls = []
        for file_path in input_folder:
            scaled_url = await workflow.execute_activity(
                "scale_tiff",
                args=[file_path, args["scale_factor"]],
                start_to_close_timeout=timedelta(seconds=3000),
            )
            scaled_urls.append(scaled_url)

        # Step 3: Compose the scaled TIFFs
        output_tiff = await workflow.execute_activity(
            "compose_tiffs",
            args=[scaled_urls],
            start_to_close_timeout=timedelta(seconds=3000),
        )

        return f"{wid}/{output_tiff}"

