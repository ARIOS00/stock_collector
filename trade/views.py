from apscheduler.schedulers.background import BackgroundScheduler
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
p = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

scheduler = BackgroundScheduler()


@scheduler.scheduled_job("interval", seconds=3)
def test():
    print("sending msg to kafka...")
    p.produce('mytopic', key='mykey', value='myvalue', callback=delivery_report)
    p.flush()


scheduler.start()


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {0}'.format(err))
    else:
        print('Message delivered to {0} [partition {1}]'
              .format(msg.topic(), msg.partition()))