import os
import tempfile
from dotenv import load_dotenv
from temporalio import activity

from activities.upload_azure_storage import upload_to_azure_storage


@activity.defn(name="download_mosdac_data")
async def download_mosdac_data(remote_path: str) -> list[str]:
    import paramiko  # ✅ Local import avoids workflow sandbox restriction

    load_dotenv()

    # SFTP Credentials
    sftp_host = "download.mosdac.gov.in"
    sftp_port = 22
    sftp_username = os.getenv("MOSDAC_USER_NAME")
    sftp_password = os.getenv("MOSDAC_PASSWORD")

    # SFTP Connection Options
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_username, password=sftp_password)

    local_path = tempfile.mkdtemp()
    with paramiko.SFTPClient.from_transport(transport) as sftp:
        print("Connected to SFTP server")
        __download_sftp_recursive__(sftp, remote_path, local_path)
        print(f"Download completed to {local_path}.")

    transport.close()

    azure_urls = upload_to_azure_storage("mosdac-par", local_path)
    return azure_urls


def __download_sftp_recursive__(sftp_client, remote_path: str, local_path: str):
    import os  # fine, just for consistency

    if not os.path.exists(local_path):
        os.makedirs(local_path)

    for entry in sftp_client.listdir_attr(remote_path):
        remote_file = f"{remote_path}/{entry.filename}"
        local_file = os.path.join(local_path, entry.filename)

        if entry.st_mode & 0o40000:  # Directory
            __download_sftp_recursive__(sftp_client, remote_file, local_file)
        else:
            print(f"Downloading {remote_file} to {local_file}")
            sftp_client.get(remote_file, local_file)
