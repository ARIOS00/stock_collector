import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from k_line.models import KLine
from datetime import datetime, date
from django.db.utils import IntegrityError
from django_redis import get_redis_connection

from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()
redis_client = get_redis_connection()

# mock browser headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/67.0.3396.99 '
                  'Safari/537.36 '
}

robot = RobotFileParser()
robot.set_url("https://finance.yahoo.com/robots.txt")
robot.read()


@scheduler.scheduled_job("interval", seconds=3)
def update_kline():
    # verify whether it is legal to fetch data from the url
    if robot.can_fetch('*', "https://finance.yahoo.com/quote/AMZN/history?p=AMZN"):
        print("robot protocol verified!")

        # stock data fetching
        r = requests.get("https://finance.yahoo.com/quote/AMZN/history?p=AMZN", headers=headers)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, 'html.parser')

        k_line = k_line_converter(soup, 0)

        latest_date = redis_client.get("latest_kline_date")
        redis_client.set("latest_kline_date", k_line.k_date)

        if latest_date is not None:
            latest_date = latest_date.decode("utf-8")
            latest_date = datetime.strptime(latest_date, "%Y-%m-%d").date()
            today_date = date.today()
            comp_days = (today_date - latest_date).days - 1
            for i in range(0, comp_days):
                k_line = k_line_converter(soup, i)
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
        print(k_line.name, '-', k_line.k_date, "save successfully!")
    except IntegrityError:
        print(k_line.name, '-', k_line.k_date, "duplicated data!")
