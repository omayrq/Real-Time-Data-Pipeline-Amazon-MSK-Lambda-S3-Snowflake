# Real-Time Data Pipeline: Amazon MSK → Lambda → S3 → Snowflake

A real-time streaming data pipeline built on AWS that ingests Kafka messages from Amazon MSK, processes them through AWS Lambda, stores partitioned JSON files in Amazon S3, and automatically loads them into Snowflake via Snowpipe — end-to-end in under 60 seconds.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                          AWS VPC                                  │
│                                                                   │
│   ┌─────────────────┐        ┌──────────────────────────────┐    │
│   │  Public Subnet  │        │      Private Subnet           │    │
│   │                 │  SSH   │                               │    │
│   │  EC2 (Bastion) ─┼───────▶  EC2 (Private)               │    │
│   │  Jump Host      │        │  Python Kafka Producer        │    │
│   └─────────────────┘        │          │                    │    │
│                               │          ▼                    │    │
│                               │   Amazon MSK Cluster          │    │
│                               │   Kafka 3.5.1 · 2 Brokers     │    │
│                               └──────────┼───────────────────┘    │
└──────────────────────────────────────────┼────────────────────────┘
                                           │
                                           │  Event Source Mapping
                                           ▼
                                   ┌───────────────┐
                                   │  AWS Lambda   │
                                   │ KafkaToS3Writer│
                                   └───────┬───────┘
                                           │  PutObject
                                           ▼
                                   ┌───────────────┐
                                   │   Amazon S3   │
                                   │  JSON Files   │
                                   └───────┬───────┘
                                           │  S3 Event Notification
                                           ▼
                                   ┌───────────────┐
                                   │  Amazon SQS   │
                                   └───────┬───────┘
                                           │  Auto-ingest trigger
                                           ▼
                                   ┌───────────────┐
                                   │   Snowflake   │
                                   │   Snowpipe    │
                                   │ real_time_demo│
                                   └───────────────┘
```

---

## Tech Stack

| Service | Purpose |
|---|---|
| Amazon MSK | Managed Apache Kafka (2 brokers, Kafka 3.5.1, IAM auth) |
| AWS Lambda | Serverless Kafka consumer — reads topic, writes to S3 |
| Amazon S3 | Data lake — stores partitioned JSON files |
| Amazon SQS | Receives S3 event notifications, triggers Snowpipe |
| Snowflake | Cloud data warehouse with Snowpipe auto-ingest |
| Amazon EC2 | Bastion host + private Kafka producer machine |
| AWS IAM | Role-based access control across all services |
| Python | Kafka producer using `kafka-python` library |

---

## Prerequisites

- AWS account with IAM permissions for MSK, Lambda, S3, SQS, EC2
- Snowflake account (free trial works)
- Python 3.9+
- SSH key pair (.pem file)

---

## Project Structure

```
├── producer.py            # Kafka message producer (runs on private EC2)
├── lambda_function.py     # Lambda handler — decodes Kafka records, writes to S3
├── snowflake_setup.sql    # All Snowflake SQL (database, table, stage, pipe)
└── README.md
```

---

## Setup Guide

### Step 1 — VPC & Networking

1. Go to **VPC → Create VPC → VPC and more**
2. Enable **NAT Gateway** (lets private EC2 reach the internet)
3. Create 2 public + 2 private subnets across 2 AZs
4. Note your subnet IDs for use in MSK and EC2 setup

---

### Step 2 — Amazon MSK Cluster

1. Go to **MSK → Create Cluster → Custom create**
2. Kafka version: `3.5.1`, broker type: `kafka.t3.small`, 2 brokers
3. Place brokers in **private subnets**
4. Under Security settings → enable **IAM role-based authentication**
5. Wait ~20 min for status to become **Active**
6. Copy bootstrap server endpoints from **View client information**

---

### Step 3 — IAM Role for Lambda

Create a role named `KafkaToS3Writer-role` and attach:

- `AmazonS3FullAccess`
- `AWSLambdaBasicExecutionRole`
- `AWSLambdaMSKExecutionRole`

---

### Step 4 — EC2 Instances

**Bastion (public EC2):**
- Public subnet, assign public IP
- Security group: allow inbound SSH port 22

**Private EC2 (Kafka producer):**
- Private subnet, no public IP
- Security group: allow outbound TCP 9092–9098 to MSK security group

**Install Kafka CLI tools on private EC2:**
```bash
sudo yum install java-1.8.0-openjdk -y
wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
tar -xvf kafka_2.12-2.8.1.tgz
cd kafka_2.12-2.8.1

# Create Kafka topic
bin/kafka-topics.sh --create \
  --topic demotesting3 \
  --bootstrap-server <YOUR_BOOTSTRAP_SERVER>:9092 \
  --replication-factor 1 \
  --partitions 2
```

---

### Step 5 — AWS Lambda Function

1. Go to **Lambda → Create Function → Author from scratch**
2. Runtime: Python 3.12
3. Assign the IAM role from Step 3
4. Place Lambda in the **same VPC and private subnets as MSK**
5. Assign a security group (`lambda-msk-sg`) with outbound TCP 9092–9098 to MSK SG

**lambda_function.py:**
```python
import json, boto3, base64
from datetime import datetime

s3 = boto3.client('s3')
BUCKET = 'your-s3-bucket-name'

def lambda_handler(event, context):
    records = []
    for partition_key, partition_records in event['records'].items():
        for record in partition_records:
            value = base64.b64decode(record['value']).decode('utf-8')
            records.append(json.loads(value))

    if records:
        now = datetime.utcnow()
        key = f"data/{now.year}/{now.month:02d}/{now.day:02d}/{now.timestamp()}.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body='\n'.join(json.dumps(r) for r in records),
            ContentType='application/json'
        )
        print(f"Wrote {len(records)} records to s3://{BUCKET}/{key}")

    return {'statusCode': 200}
```

**Add MSK trigger:**
- Source: Amazon MSK → your cluster
- Topic: `demotesting3`
- Starting position: `TRIM_HORIZON`
- Authentication: off (IAM handled by execution role)

---

### Step 6 — S3 Bucket & SQS Notification

1. Create an S3 bucket (e.g. `your-project-bucket`)
2. After completing Step 7, come back and add an **Event Notification**:
   - Event type: All object create events
   - Destination: SQS queue (use the ARN from `SHOW PIPES` in Snowflake)

---

### Step 7 — Snowflake Setup

```sql
-- Database and table
CREATE DATABASE IF NOT EXISTS s3_to_snowflake;
USE s3_to_snowflake;

CREATE OR REPLACE TABLE real_time_demo (
    data        VARIANT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- External stage pointing to your S3 bucket
CREATE OR REPLACE STAGE Snow_stage
  URL = 's3://your-s3-bucket-name/data/'
  CREDENTIALS = (
    aws_key_id     = 'YOUR_ACCESS_KEY_ID'
    aws_secret_key = 'YOUR_SECRET_ACCESS_KEY'
  );

-- Snowpipe for auto-ingest
CREATE OR REPLACE PIPE for_kafka_ingestion
  AUTO_INGEST = TRUE AS
  COPY INTO real_time_demo (data)
  FROM @Snow_stage
  FILE_FORMAT = (TYPE = 'JSON');

-- Copy the notification_channel value — use it as the SQS destination in S3
SHOW PIPES;
```

---

### Step 8 — Run the Producer

```python
# producer.py — run this on your private EC2
from kafka import KafkaProducer
from datetime import datetime
import json, time

BOOTSTRAP_SERVERS = ['<YOUR_BOOTSTRAP_SERVER>:9092']
TOPIC = 'demotesting3'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

print(f"Starting producer... sending data to {TOPIC}")
for i in range(1000):
    message = {
        'id': i,
        'source': 'ec2-producer',
        'status': 'success',
        'timestamp': datetime.utcnow().isoformat()
    }
    producer.send(TOPIC, value=message)
    print(f"Sent: {message}")
    time.sleep(0.2)

producer.flush()
print("\nBatch complete! All messages sent successfully.")
producer.close()
```

---

## Security Group Rules

| Security Group | Direction | Port | Source / Destination |
|---|---|---|---|
| MSK SG | Inbound | 9092–9098 | Lambda SG |
| MSK SG | Inbound | All | Itself (self-referencing) |
| Lambda SG | Outbound | 9092–9098 | MSK SG |
| Private EC2 SG | Outbound | All | `0.0.0.0/0` |
| Bastion SG | Inbound | 22 | Your IP |

---

## Verification Queries

```sql
-- Total records ingested
SELECT COUNT(*) FROM real_time_demo;

-- Latest records
SELECT * FROM real_time_demo ORDER BY ingested_at DESC LIMIT 20;

-- Parse JSON fields
SELECT
    parse_json(data):id::INT           AS message_id,
    parse_json(data):source::VARCHAR   AS source,
    parse_json(data):status::VARCHAR   AS status,
    parse_json(data):timestamp::VARCHAR AS event_time,
    ingested_at
FROM real_time_demo
ORDER BY message_id;
```

---

## Common Issues

| Error | Cause | Fix |
|---|---|---|
| Lambda trigger `Disabled` | MSK auth not configured | Enable IAM auth on MSK cluster |
| `NoBrokersAvailable` | Wrong port or blocked SG | Use port 9092, check EC2 outbound rules |
| Timeout on port 9094 | TLS port requires SSL config | Switch to port 9092 (plaintext) |
| S3 files not appearing | Lambda not triggered | Check Lambda CloudWatch logs |
| Snowflake table empty | SQS ARN mismatch | Verify S3 event notification uses pipe's SQS ARN |
| `kafkaConnect:CreateConnector` denied | Missing IAM permission | Attach `AmazonMSKFullAccess` to IAM user |

---

## Author

**Muhammad Umair Ashraf** — Data Engineer
