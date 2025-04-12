def upload_to_azure_storage(container_name: str, path: str) -> list[str]:
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


def download_files_from_urls(urls: list[str]) -> str:
    import tempfile
    import requests
    from urllib.parse import urlparse
    import os

    download_dir = tempfile.mkdtemp(prefix="azure_download_")
    local_paths = []

    for url in urls:
        parsed = urlparse(url)
        path_parts = parsed.path.strip("/").split("/")[1:]  # Skip container name
        local_path = os.path.join(download_dir, *path_parts)

        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        response = requests.get(url)

        if response.status_code == 200:
            with open(local_path, "wb") as f:
                f.write(response.content)
            local_paths.append(local_path)
            print(f"Downloaded: {url} -> {local_path}")
        else:
            raise Exception(f"Failed to download {url}, status code: {response.status_code}")

    if len(local_paths) == 1:
        return local_paths[0]  # Single file path
    return download_dir  # Folder containing multiple files
