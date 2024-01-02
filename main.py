from tempfile import TemporaryFile
from dotenv import load_dotenv
from google.cloud import storage
import os, json, time
from uuid import uuid4
import threading
import time
import psutil


class MemoryMonitor(threading.Thread):
    def __init__(self, interval=1):
        super().__init__()
        self.interval = interval
        self.memory_usage: list[int] = []
        self.working = False

    def run(self):
        process = psutil.Process()
        self.working = True
        while self.working:
            memory = process.memory_info().rss / (1024 * 1024)  # Convert to MB
            self.memory_usage.append(memory)
            time.sleep(self.interval)
    
    def join(self):
        self.working = False
        super().join()
    
    def get_avg_memory_usage(self):
        return sum(self.memory_usage) / len(self.memory_usage)

def main():
    load_dotenv()

    credential_json = os.getenv('GCP_CREDENTIALS_JSON')
    bucket_name = os.getenv('GCP_STORAGE_BUCKET')
    if not credential_json or not bucket_name:
        raise Exception('Missing environment variables. GCP_CREDENTIALS_JSON')
    if not bucket_name:
        raise Exception('Missing environment variables. GCP_STORAGE_BUCKET')

    credential = json.loads(credential_json)

    client = storage.Client.from_service_account_info(credential)
    bucket = client.bucket(bucket_name)

    blob_name = str(uuid4())

    blob = bucket.blob(blob_name)

    monitor = MemoryMonitor()
    monitor.start()
    with TemporaryFile('w+b') as f:
        f.write(b'x' * 1024 * 1024 * 1024)
        f.seek(0)
        blob.upload_from_file(f)
    print(f'Avg. memory usage while uplading: {monitor.get_avg_memory_usage(): .1f} MB')
    for i in range(len(monitor.memory_usage)):
        print(monitor.memory_usage[i])
    monitor.join()


if __name__ == '__main__':
    main()
