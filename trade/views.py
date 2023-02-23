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
    diff = soup.find('fin-streamer', {'data-symbol': name, 'data-field': 'regularMarketChange'}).text
    rate = soup.find('fin-streamer', {'data-symbol': name, 'data-field': 'regularMarketChangePercent'}).text
    fresh_time = soup.find('div', id='quote-market-notice').text

    diff = re.search(r'-?\d+\.\d+', diff).group()
    rate = re.search(r'-?\d+\.\d+', rate).group()
    fresh_time = re.search(r'\d+\:\d+', fresh_time).group()

    current_time = datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d")
    fresh_time = formatted_time + ' ' + fresh_time + ':00'

    data = name + ',' + price + ',' + diff + ',' + rate + ',' + fresh_time
    print(data)
    return data


@scheduler.scheduled_job("interval", seconds=10)
def produce():
    AAPL_data = fetch_trade('AAPL')
    AMZN_data = fetch_trade('AMZN')

    print("sending msg to kafka...")
    p.produce('trade', key='AAPL', value=AAPL_data, callback=delivery_report, partition=0)
    p.produce('trade', key='AMZN', value=AMZN_data, callback=delivery_report, partition=0)
    p.flush()


def consume():
    print("try to consume...")
    msg = c.poll(1.0)
    if msg is None:
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

