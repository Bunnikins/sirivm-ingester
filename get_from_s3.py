from boto3 import client
from os import environ
import logging
from concurrent.futures import ThreadPoolExecutor
import zipfile
from io import BytesIO
from datetime import datetime, timezone, timedelta
from transform_load_shared_conn import validate_data, parse_xml, init_db, insert_data
import sqlite3
from pypeln import process, thread, sync, task

S3_BUCKET = environ.get('S3_BUCKET')
HOURS_TO_EXTRACT = float(environ.get('HOURS_TO_EXTRACT'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
db_path = f'vehicles_{timestamp}.db'

def get_object(key):
    '''Download file from s3 into BytesIO and unzip file to memory'''
    try:
        logging.debug(f"Downloading file {key['Key']} from S3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key['Key'])
        zip_file = zipfile.ZipFile(BytesIO(obj['Body'].read()))
        return {
            "id": key['Key'],
            "data": zip_file.read(zip_file.namelist()[0])
        }
    except Exception as e:
        logging.error(f'Failed to download {key["Key"]}', e)

def parse_data_step(key_data):
    try:
        logging.debug(f'Parsing data for {key_data["id"]}')
        return {
            "id": key_data['id'],
            "data": parse_xml(key_data['data'], source_type='string'),
        }
    except Exception as e:
        logging.error(f'Failed to parse {key_data["id"]}', e)


def validate_data_step(parsed_data):
    try:
        logging.debug(f'Validating data for {parsed_data["id"]}')
        return {
            "id": parsed_data['id'],
            "data": validate_data(parsed_data['data'])
        }
    except Exception as e:
        logging.error(f'Failed to validate {parsed_data["id"]}', e)

def insert_data_step(validated_data):
    try:
        logging.info(f'Inserting data for {validated_data["id"]}')
        insert_data(conn, validated_data['data'])
        return validated_data['id']
    except Exception as e:
        logging.error(f'Failed to insert {validated_data["id"]}', e)
    # logging.info("Data processing and insertion completed.")

if __name__ == '__main__':
    try:
        s3 = client('s3')
        conn = init_db(db_path)
        keys = []
        earliest_time = datetime.now(timezone.utc) - timedelta(hours=HOURS_TO_EXTRACT)
        paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': S3_BUCKET,
                                'Prefix': 'sirivm_20'}
        page_iterator = paginator.paginate(**operation_parameters)
        for page in page_iterator:
            for key in page['Contents']:
                if key['LastModified'] > earliest_time:
                    keys.append(key)

        logging.info(f'Found {str(len(keys))} keys more than {str(HOURS_TO_EXTRACT)} hours old')
        stage = thread.map(get_object, keys, workers=5, maxsize=5)
        stage = process.map(parse_data_step, stage, workers=5, maxsize=5)
        stage = process.map(validate_data_step, stage, workers=5, maxsize=5)
        stage = sync.map(insert_data_step, stage, workers=1, maxsize=5)
        out=list(stage)
        conn.close()
        logging.info(f'parsed {str(len(out))} of {str(len(keys))} keys')
    except Exception as e:
        logging.error("Error fetching keys from S3", e)