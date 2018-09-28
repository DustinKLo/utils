from __future__ import print_function

import os
import io
import datetime
import StringIO
import urllib
import json

import boto3
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

from pprint import pprint

# AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
# AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
ACCESS_KEY = os.environ['ACCESS_KEY']
SECRET_KEY = os.environ['SECRET_KEY']
BUCKET_NAME = os.environ['BUCKET_NAME']
STREAM_NAME = os.environ['STREAM_NAME']

session = boto3.session.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
resource = session.resource('s3')
client = boto3.client('s3')

firehost_client = boto3.client('firehose')
bucket = resource.Bucket(BUCKET_NAME)


def add_on_dict(obj, event):
    date_obj = datetime.datetime.fromtimestamp(obj['time'])
    return {
        'time_iso': date_obj.isoformat(),
        'time_formatted': date_obj.strftime('%Y-%m-%d %H:%M:%S'),
        'event': event
    }


def create_payload(data):
    '''
    data is a list of json_stringified objects
    Records=[ {'Data': b'bytes'}, ... ]
    '''
    return [{ 'Data': json.dumps(row) } for row in data]


def create_json_s3_path(path):
    path = path.replace('.avro', '.json')
    path = path.split('/')
    path[1] = 'json'
    path = '/'.join(path)
    return path


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n): 
        yield l[i:i + n]


def main(event, context):
    for record in event['Records']:
        key = record['s3']['object']['key']
        key = urllib.unquote(key).decode('utf8')
        print(key)

        json_file_key = create_json_s3_path(key)
        print(json_file_key)
        
        obj = client.get_object(Bucket=BUCKET_NAME, Key=key)
        obj = io.BytesIO(obj['Body'].read())
        reader = DataFileReader(obj, DatumReader())
        schema = reader.datum_reader.writers_schema

        schema = schema.__dict__
        try: 
            event = schema['_props']['name'].lower()
        except:
            event = None

        converted_avro_data = [dict(row, **add_on_dict(obj=row, event=event)) for row in reader]
        json_data = json.dumps(converted_avro_data, indent=1)

        print(json.dumps(converted_avro_data[:10]))

        try:
            resource.Object(BUCKET_NAME, json_file_key).put(Body=(bytes(json_data.encode('UTF-8'))))
            print("Objects Uploaded to S3")
        except Exception as e:
            print("Objects failed to send to s3")
            print(e)


        '''
        BATCH RECORDS IN 500'S AND JSON.DUMPS EACH ROW
        '''
        if event in ['uninstall', 'conversion', 'click', 'impression', 'bounce', 'open', 'send']:         
            try:
                firehose_records = create_payload(converted_avro_data)
                firehose_records = list(divide_chunks(firehose_records, 500))

                for chunk in firehose_records:
                    response = firehost_client.put_record_batch(DeliveryStreamName=STREAM_NAME, Records=chunk) 
                
                print("Objects sent to Firehose stream: {0}".format(STREAM_NAME))
                print('{0} firehose records failed'.format(response['FailedPutCount']))
            
            except Exception as e:
                print("Objects failed to send to Firehose")
                print(e)
