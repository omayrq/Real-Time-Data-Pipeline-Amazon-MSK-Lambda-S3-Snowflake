from time import sleep
from json import dumps
from kafka import KafkaProducer

# Use port 9094 and add the security_protocol
producer = KafkaProducer(
    bootstrap_servers=[
        'b-1.mskprojectcluster.c4swic.c21.kafka.us-east-1.amazonaws.com:9094',
        'b-2.mskprojectcluster.c4swic.c21.kafka.us-east-1.amazonaws.com:9094'
    ],
    security_protocol='SSL',
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

print("Connected! Starting to send messages...")
try:
    for e in range(100):
        data = {'number': e, 'status': 'real-time-test', 'user': 'Muhammad'}
        print(f"Sending: {data}")
        producer.send('demotesting3', value=data)
        sleep(1)
except Exception as e:
    print(f"Error: {e}")