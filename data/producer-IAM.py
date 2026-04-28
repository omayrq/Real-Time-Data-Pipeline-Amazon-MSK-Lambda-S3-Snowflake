import json
import time
from kafka import KafkaProducer
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# 1. Define the IAM Token Provider class
class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        # Generates the IAM auth token for the us-east-1 region
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

# 2. Initialize the provider
tp = MSKTokenProvider()

# 3. Configure the Producer
# Note: Using Port 9098 for IAM Authentication
producer = KafkaProducer(
    bootstrap_servers=[
        'b-1.mskprojectcluster.c4swic.c21.kafka.us-east-1.amazonaws.com:9098', 
        'b-2.mskprojectcluster.c4swic.c21.kafka.us-east-1.amazonaws.com:9098'
    ],
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
    client_id='my-ec2-producer',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 4. Sending Logic
topic_name = 'demotesting3'

print(f"Starting producer... sending data to {topic_name}")

try:
    for i in range(10):
        # Creating a sample data packet
        data = {
            "id": i, 
            "source": "ec2-producer", 
            "status": "success",
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Send data to MSK
        producer.send(topic_name, value=data)
        print(f"Sent: {data}")
        
        time.sleep(1) # Wait 1 second between messages

    # Ensure all messages are physically sent before closing
    producer.flush()
    print("\nBatch complete! All messages sent successfully.")

except Exception as e:
    print(f"\n[ERROR] Could not send message: {e}")

finally:
    producer.close()