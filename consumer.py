from kafka import KafkaConsumer
from kafka import TopicPartition

KAFKA_TOPIC_NAME='PYTHONTOPIC'
KAFKA_CONSUMER_GROUP='KAFKA_CONSUMER_GROUP'
consumer = KafkaConsumer(
    KAFKA_TOPIC_NAME,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
group_id=KAFKA_CONSUMER_GROUP

)
for message in consumer:
    print(message.value)
    consumer.commit()    # <--- This is what we need
    # Optionally, To check if everything went good
    print('New Kafka offset: %s' % consumer.committed(TopicPartition(KAFKA_TOPIC_NAME, message.partition)))

#thanks to theses guys : https://stackoverflow.com/questions/24661533/kafka-consumer-how-to-start-consuming-from-the-last-message-in-python
# Take care with the kafka-python library. It has a few minor issues.
#
# If speed is not really a problem for your consumer you can set the auto-commit in every message. It should works.
#
# SimpleConsumer provides a seek method (https://github.com/mumrah/kafka-python/blob/master/kafka/consumer/simple.py#L174-L185) that allows you to start consuming messages in whatever point you want.
#
# The most usual calls are:
#
# consumer.seek(0, 0) to start reading from the beginning of the queue.
# consumer.seek(0, 1) to start reading from current offset.
# consumer.seek(0, 2) to skip all the pending messages and start reading only new messages.
# The first argument is an offset to those positions. In that way, if you call consumer.seek(5, 0) you will skip the first 5 messages from the queue.
#
# Also, don't forget, the offset is stored for consumer groups. Be sure you are using the same one all the time.
