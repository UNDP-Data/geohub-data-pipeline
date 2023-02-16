from azure.storage.blob import ContainerClient
from rasterio.session import AzureSession

from .config import account_url, azure_storage_access_key, container_name


def azure_container_client():
    container_client = ContainerClient(
        account_url=account_url,
        container_name=container_name,
        credential=azure_storage_access_key,
    )
    return container_client
