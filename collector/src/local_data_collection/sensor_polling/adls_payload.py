import logging
import os
import uuid
from datetime import UTC, datetime

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import find_dotenv, load_dotenv

from local_data_collection.sensor_polling.models import HardwarePayload
from local_data_collection.utils import config_loader, machine_uuid

logger = logging.getLogger(__name__)


class UploadMemory:
    def __init__(self) -> None:
        self.count = 0
        self.payload = HardwarePayload()

    def payload_buffer_add(
        self, filename: str, cpu_payload: list, gpu_payload: list, memory_payload: list, test_id: str
    ) -> None:
        combined_tuple = cpu_payload + gpu_payload + memory_payload
        if not self.payload.metadata and not self.payload.snapshots:
            software_config = config_loader.load_conf(filename)
            windows_device_uuid = machine_uuid.get_windows_uuid()

            self.payload.metadata["underlying_system"] = "LibreHardwareMonitor"
            self.payload.metadata["testing_software"] = software_config
            self.payload.metadata["device_id"] = windows_device_uuid
            self.payload.metadata["test_id"] = test_id
            self.payload.metadata["test_date"] = datetime.now(UTC).strftime(r"%Y%m%d_%H%M%S")
            logger.info("Payload metadata initialzied")
        else:
            logger.info("Sensor data snapshot appended")

        self.payload.snapshots.extend(combined_tuple)
        self.count += 1

    def check_buffer(self, buffer_size_limit: int) -> bool:
        if len(self.payload.snapshots) >= buffer_size_limit:
            logger.info(f"buffer size limit ({buffer_size_limit}) reached")
            return True
        return False

    def clear_buffer(self) -> None:
        self.payload.snapshots.clear()
        self.count = 0
        logger.info("Payload snapshots list cleared")


def adls_credentials(environment: str | None = None) -> tuple[ClientSecretCredential, dict]:
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)

    client_id = os.getenv("AZURE_CLIENT_ID")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")

    entered_environemnt = environment or os.getenv("ENVIRONMENT")

    if entered_environemnt is None:
        raise ValueError("Selected environment is None")

    dev_environment = entered_environemnt.lower()
    if dev_environment == "dev":
        env_mapping = {
            "CONTAINER": os.getenv("DEV_LANDING_CONTAINER"),
            "STORAGE_URL": os.getenv("DEV_AZURE_STORAGE_URL"),
        }
    elif dev_environment == "test":
        env_mapping = {
            "CONTAINER": os.getenv("TEST_LANDING_CONTAINER"),
            "STORAGE_URL": os.getenv("TEST_AZURE_STORAGE_URL"),
        }
    elif dev_environment == "prod":
        env_mapping = {
            "CONTAINER": os.getenv("PROD_LANDING_CONTAINER"),
            "STORAGE_URL": os.getenv("PROD_AZURE_STORAGE_URL"),
        }
    else:
        raise ValueError("Environemnt entered is not in pre-configured environment options")
    logger.info(f"Current environment: {dev_environment} retrieved")

    if client_id is None or client_secret is None or tenant_id is None:
        raise ValueError(f"Values {client_id} or {client_secret} or {tenant_id} is None")

    credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

    return credentials, env_mapping


def upload_blob(credentials: ClientSecretCredential, env_mapping: dict, sensor_buffer: HardwarePayload) -> None:
    blob_service_client = BlobServiceClient(account_url=env_mapping["STORAGE_URL"], credential=credentials)
    container_client = blob_service_client.get_container_client(container=env_mapping["CONTAINER"])

    HardwarePayload.model_validate(sensor_buffer.model_dump())
    sensors_json = sensor_buffer.model_dump_json()

    timestamp = datetime.now(UTC).strftime(r"%Y%m%d_%H%M%S")
    datefolder = datetime.now(UTC).strftime(r"%Y-%m-%d")
    event_id = uuid.uuid4()

    blob_name = f"incoming/ingest_date={datefolder}/hardware_sensor_data_{timestamp}_{event_id}.json"
    blob_client = container_client.get_blob_client(blob=blob_name)
    blob_client.upload_blob(data=sensors_json, content_settings=ContentSettings(content_type="application/json"))
    logger.info(f"Sucessfully uploaded blob {blob_name}")
