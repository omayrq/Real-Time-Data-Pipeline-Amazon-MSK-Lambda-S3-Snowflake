import json
import boto3
import base64
from datetime import datetime

s3 = boto3.client('s3')
# UPDATE THIS: Use your actual bucket name
BUCKET_NAME = 'irisseta-mua' 

def lambda_handler(event, context):
    # MSK sends data in a 'records' dictionary
    all_records = []
    
    for topic_partition, records in event['records'].items():
        for record in records:
            # Kafka messages are Base64 encoded
            payload = base64.b64decode(record['value']).decode('utf-8')
            all_records.append(json.loads(payload))

    if all_records:
        now = datetime.utcnow()
        # This creates the same folder structure the connector would have
        file_key = f"data/{now.strftime('%Y/%m/%d')}/{now.timestamp()}.json"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=file_key,
            Body='\n'.join(json.dumps(r) for r in all_records),
            ContentType='application/json'
        )
        print(f"Successfully wrote {len(all_records)} records to {file_key}")

    return {'statusCode': 200}