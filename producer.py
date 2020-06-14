from kafka import KafkaProducer,KafkaClient
import json
import time
#kafka_client = KafkaClient('localhost:9092')

#kafka_client.ensure_topic_exists('PYTHON_TOPIC')
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
print("Start")
for key in range(1000):
    future = producer.send('PYTHONTOPIC',json.dumps({'key': key, 'index': key}))
    result = future.get(timeout=60)
    print (key,result)
    if key%100==0:
        time.sleep(10)
print("End")
