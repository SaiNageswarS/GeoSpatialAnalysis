from temporalio import activity


@activity.defn(name="upload_azure_storage")
async def upload_azure_storage(container_name: str, path: str) -> list[str]:
    import os
    from azure.storage.blob import BlobServiceClient, ContentSettings
    from dotenv import load_dotenv

    load_dotenv()

    account_name = os.getenv("AZURE_STORAGE_ACCOUNT")
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    if not container_client.exists():
        container_client.create_container()

    public_urls = []

    if os.path.isfile(path):
        # Handle single file
        file_name = os.path.basename(path)
        with open(path, "rb") as data:
            container_client.upload_blob(
                name=file_name,
                data=data,
                overwrite=True,
                content_settings=ContentSettings(content_type=None)
            )
        url = f"https://{account_name}.blob.core.windows.net/{container_name}/{file_name}"
        public_urls.append(url)

    elif os.path.isdir(path):
        # Handle folder
        for root, _, files in os.walk(path):
            for file in files:
                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, path)
                blob_path = relative_path.replace("\\", "/")  # Normalize for Azure

                with open(local_file_path, "rb") as data:
                    container_client.upload_blob(
                        name=blob_path,
                        data=data,
                        overwrite=True,
                        content_settings=ContentSettings(content_type=None)
                    )

                url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_path}"
                public_urls.append(url)
    else:
        raise FileNotFoundError(f"Path does not exist: {path}")

    return public_urls

