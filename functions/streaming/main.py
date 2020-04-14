import json
import os
import traceback
import logging
from datetime import datetime

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage
import pytz

PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'dataset'
CS = storage.Client()
BQ = bigquery.Client()
DB = firestore.Client()

def streaming(data, context):
    '''This function is executed whenever a file is added to GCS'''
    bucket_name = data['bucket']
    file_name = data['name']
    db_ref =  DB.document(f'streaming_files/{file_name}')
    
    try:
        _insert_into_bq(bucket_name, file_name)
        _handle_success(db_ref)
    except Exception:
        _handle_error(db_ref)

def _insert_into_bq(bucket_name, file_name):
    category = _get_category(file_name)
    blob = CS.get_bucket(bucket_name).blob(file_name)
    row = json.loads(blob.download_as_string())

    if category == 'dcs':
        dataset_name = 'dcs'
    elif category == 'spectral':
        dataset_name = 'spectral'
    else:
        dataset_name = 'quality'

    table = BQ.dataset(BQ_DATASET).table(dataset_name)
    errors = BQ.insert_rows_json(table,
        json_rows=[row], row_ids=[file_name], retry=retry.Retry(deadline=30))
    
    if errors != []:
        raise BigQueryError(errors)

def _get_category(file_name):
    if file_name.find('dcs') != -1:
        return 'dcs'
    elif file_name.find('spectral') != -1:
        return 'spectral'
    elif file_name.find('quality') != -1:
        return 'quality'
    else:
        raise NotImplementedError()

def _handle_success(db_ref):
    message = 'File \'%s\' streamed into BigQuery' % db_ref.id
    doc = {
        u'success': True,
        u'when': _now()
    }
    db_ref.set(doc)
    logging.info(message)


def _handle_error(db_ref):
    message = 'Error streaming file \'%s\'. Cause: %s' % (db_ref.id, traceback.format_exc())
    doc = {
        u'success': False,
        u'error_message': message,
        u'when': _now()
    }
    db_ref.set(doc)
    logging.error(message)

def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')

class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened''' 

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)