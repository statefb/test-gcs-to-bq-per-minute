import os
import time
import numpy as np
import json
from datetime import datetime

from google.cloud import storage
from google.oauth2.service_account import Credentials

from generator import DcsGenerator, QualityGenerator, SpectralGenerator

REGION = 'asia-northeast1'
BUCKET_NAME = 'tutorial-gcs-to-bq-files'
PROJECT = 'tutorial-gcs-to-bq'
CREDENTIALS = Credentials.from_service_account_file(filename='tutorial-gcs-to-bq-274201-21e8dcd719f0.json')
CS = storage.Client(project=PROJECT, credentials=CREDENTIALS)
FORMAT = "%Y-%m-%d %H:%M:%S"

def test_put():
    file_name = 'test_file'

    bucket = CS.get_bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)

    json_string = """
    [
        {
            "name": "date",
            "type": "STRING",
            "mode": "NULLABLE"
        }
    ]
    """
    blob.upload_from_string(json_string)

def _put_to_gcs(file_name, string):
    bucket = CS.get_bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    blob.upload_from_string(string)


def put_to_gcs():
    dcs_str = DcsGenerator().generate()
    quality_str = QualityGenerator().generate()
    spectral_str = SpectralGenerator().generate()
    now = datetime.now().strftime(FORMAT)

    _put_to_gcs(f"{now}_dcs.json", dcs_str)
    _put_to_gcs(f"{now}_quality.json", quality_str)
    _put_to_gcs(f"{now}_spectral.json", spectral_str)

if __name__ == "__main__":
    while True:
        put_to_gcs()
        time.sleep(60)
    