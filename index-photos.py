import json
import boto3
import time
import requests
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import imghdr

OPENSEARCH_ENDPOINT = "https://search-photos-lwaf6zvsq74hrr6gmapeqmbnzm.us-east-1.es.amazonaws.com/photos/"
REGION = 'us-east-1'
HOST = 'search-photos-lwaf6zvsq74hrr6gmapeqmbnzm.us-east-1.es.amazonaws.com'
INDEX = 'photos'

def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
        cred.secret_key,
        region,
        service,
        session_token=cred.token
    )

# DEFINING 
s3_client = boto3.client('s3')
reko_client = boto3.client('rekognition', region_name=REGION)
os_client = OpenSearch(
    hosts=[{ 'host': HOST, 'port': 443 }],
    http_auth=get_awsauth(REGION, 'es'),
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

def lambda_handler(event, context):
    print("event: ", event)
    print("context: ", context)
    
    s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    s3_img = event["Records"][0]["s3"]["object"]["key"]
    
    # === REKOGNITION ===
    print("Bucket and Image Name: ")
    print(s3_bucket, s3_img)
    reko_labels = reko_client.detect_labels(Image={'S3Object':{'Bucket':s3_bucket,'Name':s3_img}})
    
    print("reko_labels: ", reko_labels)
    
    # === S3 -> X-AMZ ===
    s3_res = s3_client.head_object(Bucket=s3_bucket, Key=s3_img)
    print("s3_res: ", s3_res)
    if 'x-amz-meta-customLabels' in s3_res['Metadata']:
        custom_labels_metadata = response['Metadata']['x-amz-meta-customLabels']
        custom_labels_json = json.loads(custom_labels_metadata)
        print("x-amz-meta-customLabels: ", custom_labels_json)
    else:
        print("x-amz-meta-customLabels metadata field not found.")
    
    # === OPENSEARCH --> ADD DATA ===
    try:
        # extract data to push to opensearch
        data = {}
        data["objectKey"] = s3_img
        data["bucket"] = s3_bucket
        data["createdTimestamp"] = time.time()
        data['labels'] = []
        for label in reko_labels['Labels']:
            data['labels'].append(label['Name'])
        json_data = json.dumps(data)
        print("opensource data stored: ", json_data)
        
        # post request
        os_res = os_client.index(index=INDEX, body=json_data)
        print("opensource request: ", os_res)
        
    except Exception as e:
        print("Errorssssssssss: "+ str(e))
    
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'event': event
        }),
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "PUT,OPTIONS"
        },
    }