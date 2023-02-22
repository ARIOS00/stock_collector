from apscheduler.schedulers.background import BackgroundScheduler
from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
from datetime import datetime
import requests
import re
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

p = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
c = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['trade'])

scheduler = BackgroundScheduler()

# mock browser headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/67.0.3396.99 '
                  'Safari/537.36 '
}

robot = RobotFileParser()


def fetch_trade(name):
    # verify whether it is legal to fetch data from the url
    robot.set_url("https://finance.yahoo.com/robots.txt")
    robot.read()
    if not robot.can_fetch('*', "https://finance.yahoo.com/quote/{name}?p={name}&.tsrc=fin-srch".format(name=name)):
        print("robot protocol not allowed!")
        return

    # stock data fetching
    r = requests.get("https://finance.yahoo.com/quote/{name}?p={name}&.tsrc=fin-srch".format(name=name), headers=headers)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, 'html.parser')
    price = soup.find('fin-streamer', {'data-symbol': name, 'data-field': 'regularMarketPrice'}).text
    change = soup.find('fin-streamer', {'data-symbol': name, 'data-field': 'regularMarketChange'}).text
    rate = soup.find('fin-streamer', {'data-symbol': name, 'data-field': 'regularMarketChangePercent'}).text

    change = re.search(r'-?\d+\.\d+', change).group()
    rate = re.search(r'-?\d+\.\d+', rate).group()

    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H:%M:%S")

    return [price, change, rate, formatted_time]


@scheduler.scheduled_job("interval", seconds=50)
def produce():
    name = 'AAPL'
    data = fetch_trade(name)
    data = data[0] + ',' + data[1] + ',' + data[2] + ',' + data[3]

    print("sending msg to kafka...")
    p.produce('trade', key=name, value=data, callback=delivery_report)
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