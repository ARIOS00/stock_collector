from apscheduler.schedulers.background import BackgroundScheduler
from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

p = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
c = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['mytopic'])

scheduler = BackgroundScheduler()


@scheduler.scheduled_job("interval", seconds=5)
def produce():
    print("sending msg to kafka...")
    p.produce('mytopic', key='mykey', value='myvalue', callback=delivery_report)
    p.flush()


@scheduler.scheduled_job("interval", seconds=3)
def consume():
    print("try to consume...")
    msg = c.poll(1.0)
    if msg is None:
        print("no message detected!")
        return
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print('Error while consuming message: {0}'.format(msg.error()))
    else:
        print('Received message: key={0}, value={1}, topic={2}, partition={3}, offset={4}'
              .format(msg.key().decode('utf-8'), msg.value().decode('utf-8'), msg.topic(), msg.partition(),
                      msg.offset()))

scheduler.start()


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {0}'.format(err))
    else:
        print('Message delivered to {0} [partition {1}]'
              .format(msg.topic(), msg.partition()))