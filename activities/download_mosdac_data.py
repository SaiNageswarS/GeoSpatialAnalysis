import os
import tempfile
import logging

from typing import List
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def download_mosdac_data(remote_path: str) -> list[Path]:
    """
    Recursively download every *.tif file found under `remote_path` on
    MOSDAC's SFTP server into a *single* temporary directory.

    Returns a list of Path objects pointing to the local copies.
    """

    import paramiko

    sftp_host = "download.mosdac.gov.in"
    sftp_port = 22
    sftp_username = os.environ["MOSDAC_USER_NAME"]
    sftp_password = os.environ["MOSDAC_PASSWORD"]

    # One temp directory that will contain all TIFFs flat.
    local_root = Path(tempfile.mkdtemp(prefix="mosdac_flat_"))

    transport = paramiko.Transport((sftp_host, sftp_port))
    try:
        transport.connect(username=sftp_username, password=sftp_password)
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            logger.info("✅ Connected to SFTP server")
            downloaded = _collect_tifs_flat(sftp, remote_path, local_root)
            logger.info(f"📥  Downloaded {len(downloaded)} files into {local_root}")
            return downloaded
    finally:
        transport.close()


def _collect_tifs_flat(
        sftp,
        remote_path: str,
        local_root: Path,
) -> list[Path]:
    """
    Walk `remote_dir`; for each *.tif file, download it into `local_root`.

    `name_counts` keeps track of how many times we've already used a given
    base filename so we can disambiguate clashes.
    """
    result: list[Path] = []

    logger.info(f"Listing {remote_path}")
    for entry in sftp.listdir_attr(remote_path):
        remote_file = f"{remote_path}/{entry.filename}"

        if not entry.filename.lower().endswith(".tif"):
            continue  # skip non-TIFFs

        local_file = local_root.joinpath(entry.filename)
        sftp.get(remote_file, str(local_file))

        result.append(local_file)

    return result
