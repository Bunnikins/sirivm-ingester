from boto3 import client
from os import environ
import logging
from concurrent.futures import ThreadPoolExecutor
import zipfile
from io import BytesIO
from datetime import datetime
from transform_load import validate_data,extract_data,parse_xml,init_db, insert_data, init_cache
import sqlite3

S3_BUCKET = environ.get('S3_BUCKET')

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

def process_data(key):
    try:
        data = get_object(key)
        cache = init_cache(db_path)  # Initialize the cache
        parsed_data = parse_xml(data, source_type='string')
        valid_data = validate_data(parsed_data)
        insert_data(db_path, valid_data, cache)  # Pass the cache to insert_data
    except Exception as e:
        logging.error("Error processing data", e)
    # logging.info("Data processing and insertion completed.")

try:
    init_db(db_path)
    keys = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='sirivm', MaxKeys=1)['Contents']
    with ThreadPoolExecutor(max_workers=5) as executor:
        returned_data = list(executor.map(process_data, keys))
        print(returned_data)
except:
    logging.error("Error fetching keys from S3")