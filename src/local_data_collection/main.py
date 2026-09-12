import time
import concurrent.futures
import logging
import uuid

from local_data_collection.sensor_polling.hardware_sensor_collectors import init_computer, get_cpu_data, get_gpu_data, get_memory_data
from local_data_collection.sensor_polling.adls_payload import UploadMemory, adls_credentials, upload_blob
from local_data_collection.utils import config_loader, logging_ldc

logging_ldc.logging_setup()

def main():
    test_id = str(uuid.uuid4())

    try:
        computer, hardwaretype = init_computer(cpu=True, gpu=True, motherboard=True, memory=True)
        testing_configs = config_loader.load_conf("occt_software.yml")
        end_time = time.time() + 60 * testing_configs["occt"]["stability_test"]["length_in_minutes"]
        mainmemory = UploadMemory()
        credential, env_mapping = adls_credentials()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            while time.time() < end_time:
                check_buffer_result = mainmemory.check_buffer(buffer_size_limit=200000)
                if check_buffer_result == False:
                    f1 = executor.submit(get_memory_data, computer, hardwaretype)
                    f2 = executor.submit(get_gpu_data, computer, hardwaretype)
                    f3 = executor.submit(get_cpu_data, computer, hardwaretype)

                    memory_data = f1.result()
                    gpu_data = f2.result()
                    cpu_data = f3.result()

                    mainmemory.payload_buffer_add(filename="occt_software.yml", 
                                                cpu_payload=cpu_data,
                                                    gpu_payload=gpu_data, 
                                                    memory_payload=memory_data,
                                                    test_id=test_id)
                else:
                    upload_blob(credentials=credential, env_mapping=env_mapping, sensor_buffer=mainmemory.payload)

                    mainmemory.clear_buffer()

        if mainmemory.payload.snapshots:
            upload_blob(credentials=credential, env_mapping=env_mapping, sensor_buffer=mainmemory.payload)
    except Exception as e:
        logging.exception(f"Error in pipeline run: {e} test id: {test_id}")
        raise

if __name__ == "__main__":
    main()
        