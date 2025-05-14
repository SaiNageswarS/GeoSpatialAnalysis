from temporalio import workflow

from activities.compose_tiff import compose_tiff
from activities.download_mosdac_data import download_mosdac_data
from activities.scale_tiff import scale_tiff
from datetime import timedelta


@workflow.defn(name="ProcessMosdac")
class ProcessMosdac:
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
        scaled_urls = []
        for file_path in input_folder:
            scaled_url = await workflow.execute_activity(
                scale_tiff,
                args=[file_path, "mosdac-par", args["scale_factor"]],
                start_to_close_timeout=timedelta(seconds=300),
            )
            scaled_urls.append(scaled_url)

        # Step 3: Compose the scaled TIFFs
        output_tiff = await workflow.execute_activity(
            compose_tiff,
            args=[scaled_urls],
            start_to_close_timeout=timedelta(seconds=300),
        )

        return output_tiff
