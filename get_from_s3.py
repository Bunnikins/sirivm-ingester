from boto3 import client
from os import environ
import logging
from concurrent.futures import ThreadPoolExecutor
import zipfile
from io import BytesIO
from datetime import datetime, timezone, timedelta
from transform_load import validate_data,extract_data,parse_xml,init_db, insert_data, init_cache
import sqlite3
from pypeln import process, thread, sync

S3_BUCKET = environ.get('S3_BUCKET')
HOURS_TO_EXTRACT = float(environ.get('HOURS_TO_EXTRACT'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
db_path = f'vehicles_{timestamp}.db'

s3 = client('s3')

def get_object(key):
    '''Download file from s3 into BytesIO and unzip file to memory'''
    try:
        logging.info(f"Downloading file {key['Key']} from S3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key['Key'])
        zip_file = zipfile.ZipFile(BytesIO(obj['Body'].read()))
        return zip_file.read(zip_file.namelist()[0])
    except Exception as e:
        print('Error:', e)

def parse_data_step(key_data):
        logging.info('Parsing data')
        return parse_xml(key_data, source_type='string')

def validate_data_step(parsed_data):
        logging.info('Validating data')
        return validate_data(parsed_data)

def insert_data_step(validated_data):
    try:
        logging.info('Creating cache')
        cache = init_cache(db_path)  # Initialize the cache
        logging.info('Inserting Data')
        insert_data(db_path, validated_data, cache)  # Pass the cache to insert_data
    except Exception as e:
        logging.error("Error processing data", e)
    # logging.info("Data processing and insertion completed.")

if __name__ == '__main__':
    try:
        keys = []
        earliest_time = datetime.now(timezone.utc) - timedelta(hours=HOURS_TO_EXTRACT)
        init_db(db_path)
        paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': S3_BUCKET,
                                'Prefix': 'sirivm_20'}
        page_iterator = paginator.paginate(**operation_parameters)
        for page in page_iterator:
            for key in page['Contents']:
                if key['LastModified'] > earliest_time:
                    keys.append(key)

        logging.info(f'Found {str(len(keys))} keys more than x hours old')
        stage = thread.map(get_object, keys, workers=5, maxsize=5)
        stage = process.map(parse_data_step, stage, workers=5, maxsize=5)
        stage = process.map(validate_data_step, stage, workers=5, maxsize=5)
        stage = thread.map(insert_data_step, stage, workers=1, maxsize=5)
        out=list(stage)
    except Exception as e:
        logging.error("Error fetching keys from S3", e)