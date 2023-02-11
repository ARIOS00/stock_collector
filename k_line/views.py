import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from k_line.models import KLine
from datetime import datetime
from django.db.utils import IntegrityError
from datetime import date

from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()

# mock browser headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/67.0.3396.99 '
                  'Safari/537.36 '
}

robot = RobotFileParser()
robot.set_url("https://finance.yahoo.com/robots.txt")
robot.read()


@scheduler.scheduled_job("interval", seconds=5)
def update_kline():
    # verify whether it is legal to fetch data from the url
    if robot.can_fetch('*', "https://finance.yahoo.com/quote/GE/history?p=GE"):
        print("robot protocol verified!")

        # AMZN data fetching
        r = requests.get("https://finance.yahoo.com/quote/AMZN/history?p=AMZN", headers=headers)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, 'html.parser')
        k_line = k_line_converter(soup, 0)
        save(k_line)
        
    else:
        print("robot protocol violated!")


scheduler.start()


def k_line_converter(soup, index):
    offset = index * 7

    # k_date conversion
    date_obj = datetime.strptime(soup.select('td')[0 + offset].text, '%b %d, %Y')
    k_date = datetime.strftime(date_obj, '%Y-%m-%d')

    close = float(soup.select('td')[4 + offset].text)
    volume = float(soup.select('td')[6 + offset].text.replace(',', ''))
    open = float(soup.select('td')[1 + offset].text)
    high = float(soup.select('td')[2 + offset].text)
    low = float(soup.select('td')[3 + offset].text)

    kline = KLine(
        name='AMZN',
        k_date=k_date,
        close=close,
        volume=volume,
        open=open,
        high=high,
        low=low
    )

    return kline


def save(k_line):
    try:
        k_line.save()
        print("save successfully!")
    except IntegrityError:
        print("duplicated data!")
