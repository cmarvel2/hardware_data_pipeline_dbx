from datetime import datetime, timezone
import logging
import os
import uuid
 
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv, find_dotenv

from local_data_collection.sensor_polling.models import HardwarePayload
from local_data_collection.utils import config_loader, machine_uuid

class UploadMemory:
    def __init__(self) -> None:
        self.count = 0
        self.payload = HardwarePayload()

    def payload_buffer_add(self, filename: str, cpu_payload: list, gpu_payload: list, memory_payload: list, test_id: str) -> None:
        combined_tuple = (cpu_payload + gpu_payload + memory_payload)
        if not self.payload.metadata and not self.payload.snapshots:
            software_config = config_loader.load_conf(filename)
            windows_device_uuid = machine_uuid.get_windows_uuid()

            self.payload.metadata["underlying_system"] = "LibreHardwareMonitor"
            self.payload.metadata["testing_software"] = software_config
            self.payload.metadata["device_id"] = windows_device_uuid
            self.payload.metadata["test_id"] = test_id
            self.payload.metadata["test_date"] = datetime.now(timezone.utc).strftime(r"%Y%m%d_%H%M%S")
            logging.info("Payload metadata initialzied")
        else:
            logging.info("Sensor data snapshot appended")

        self.payload.snapshots.extend(combined_tuple)
        self.count += 1

    def check_buffer(self, buffer_size_limit: int) -> bool:
        if len(self.payload.snapshots) >= buffer_size_limit:
            logging.info(f"buffer size limit ({buffer_size_limit}) reached")
            return True
        else:
            return False

    def clear_buffer(self) -> None:
        self.payload.snapshots.clear()
        self.count = 0
        logging.info("Payload snapshots list cleared")

def adls_credentials(environment: str | None=None) -> tuple[ClientSecretCredential, dict]:
    dotenv_path = find_dotenv()
    load_dotenv(dotenv_path)

    client_id = os.getenv("AZURE_CLIENT_ID")
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")

    dev_environment = (environment or os.getenv("ENVIRONMENT")).lower()
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
    logging.info(f"Current environment: {dev_environment} retrieved")

    credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)

    return credentials, env_mapping

def upload_blob(credentials: ClientSecretCredential, env_mapping: dict, sensor_buffer: HardwarePayload) -> None:
    blob_service_client = BlobServiceClient(account_url=env_mapping["STORAGE_URL"], credential=credentials)
    container_client = blob_service_client.get_container_client(container=env_mapping["CONTAINER"])

    HardwarePayload.model_validate(sensor_buffer.model_dump())
    sensors_json = sensor_buffer.model_dump_json()

    timestamp = datetime.now(timezone.utc).strftime(r"%Y%m%d_%H%M%S")
    datefolder = datetime.now(timezone.utc).strftime(r"%Y-%m-%d")
    event_id = uuid.uuid4()

    blob_name = f"ingest_date={datefolder}/hardware_sensor_data_{timestamp}_{event_id}.json"
    blob_client = container_client.get_blob_client(blob=blob_name)
    blob_client.upload_blob(data=sensors_json, content_settings=ContentSettings(content_type="application/json"))
    logging.info(f"Sucessfully uploaded blob {blob_name}")
    