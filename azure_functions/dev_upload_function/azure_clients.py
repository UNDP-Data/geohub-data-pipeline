import azure.storage.blob
from rasterio.session import AzureSession

from .config import (
    account_url,
    azure_account,
    azure_storage_access_key,
    container_name,
    credential_string,
)


def azure_container_client():
    container_client = azure.storage.blob.ContainerClient(
        account_url=account_url,
        container_name=container_name,
        credential=credential_string,
    )
    return container_client


def rasterio_az_session():
    azure_session = AzureSession(
        azure_storage_connection_string=None,
        azure_storage_account=azure_account,
        azure_storage_access_key=azure_storage_access_key,
        azure_unsigned=False,
    )
    return azure_session
