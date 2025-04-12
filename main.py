import os
import asyncio
from datetime import timedelta
from dotenv import load_dotenv

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

from activities.compose_tiff import compose_tiff
from activities.download_mosdac_data import download_mosdac_data
from activities.scale_tiff import scale_tiff
from activities.upload_azure_storage import upload_azure_storage
from util import connect_with_backoff


@workflow.defn(name="GeoSpatialAnalysis")
class GeoSpatialAnalysis:
    @workflow.run
    async def run(self, args) -> list[str]:
        print(f"🚨 Workflow Args: {args} ({type(args)})")

        # Step 1: Download the data
        input_folder = await workflow.execute_activity(
            download_mosdac_data,
            args=[args["remote_path"]],
            start_to_close_timeout=timedelta(seconds=300),
        )

        # Step 2: Scale the TIFF files
        scaled_folder = await workflow.execute_activity(
            scale_tiff,
            args=[input_folder, args["scale_factor"]],
            start_to_close_timeout=timedelta(seconds=300),
        )

        # Step 3: Compose the scaled TIFFs
        output_tiff = await workflow.execute_activity(
            compose_tiff,
            args=[scaled_folder],
            start_to_close_timeout=timedelta(seconds=300),
        )

        azure_path = await workflow.execute_activity(
            upload_azure_storage,
            args=[output_tiff],
            start_to_close_timeout=timedelta(seconds=300),
        )

        return azure_path


async def main():
    load_dotenv()

    temporal_host = os.getenv("TEMPORAL_SERVER")

    client = await connect_with_backoff(temporal_host)
    worker = Worker(
        client,
        task_queue="GeoSpatialAnalysisQueue",
        workflows=[GeoSpatialAnalysis],
        activities=[download_mosdac_data, scale_tiff, compose_tiff],
    )

    print("🚀 Starting Temporal Worker...")
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())
