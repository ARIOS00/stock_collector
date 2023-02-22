import requests
import threading
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from k_line.models import KLine
from datetime import datetime, date
from django.db.utils import IntegrityError
from django_redis import get_redis_connection
from apscheduler.schedulers.background import BackgroundScheduler
from controls import KlineThreadControl

scheduler = BackgroundScheduler()
redis_client = get_redis_connection()
lock = threading.Lock()
control = KlineThreadControl(2, 0)
redis_date_list = list()

# mock browser headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/67.0.3396.99 '
                  'Safari/537.36 '
}

robot = RobotFileParser()


def update_kline(name):
    # verify whether it is legal to fetch data from the url
    robot.set_url("https://finance.yahoo.com/robots.txt")
    robot.read()
    if robot.can_fetch('*', "https://finance.yahoo.com/quote/{name}/history?p={name}".format(name=name)):
        print("robot protocol verified!")

        # stock data fetching
        r = requests.get("https://finance.yahoo.com/quote/{name}/history?p={name}".format(name=name), headers=headers)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, 'html.parser')

        latest_date = redis_client.get("latest_kline_date")

        if latest_date is not None:
            latest_date = latest_date.decode("utf-8")
            latest_date = datetime.strptime(latest_date, "%Y-%m-%d").date()
            today_date = date.today()
            comp_days = (today_date - latest_date).days - 1
            k_line = None
            for i in range(0, comp_days):
                try:
                    k_line = k_line_converter(soup, name, comp_days-i-1)
                    save(k_line)

                    # store latest date to redis
                    # redis_client.set("latest_kline_date", k_line.k_date)
                except Exception:
                    print("save {name} No.{i} failed!".format(name=name, i=i))
            return None if k_line is None else k_line.k_date
        else:
            print("key latest_kline_date is missing in redis!")
    else:
        print("robot protocol violated!")

    return None


@scheduler.scheduled_job("interval", seconds=9999999)
def update_AMZN():
    update_redis(update_kline("AMZN"))


@scheduler.scheduled_job("interval", seconds=9999999)
def update_TSLA():
    update_redis(update_kline("TSLA"))


scheduler.start()


def update_redis(k_date):
    with lock:
        control.current_finished_num += 1
        if k_date is None:
            return
        if len(redis_date_list) == 0:
            redis_date_list.append(k_date)
        else:
            if k_date < redis_date_list[0]:
                redis_date_list.pop()
                redis_date_list.append(k_date)
        if control.current_finished_num == control.thread_count:
            redis_client.set("latest_kline_date", redis_date_list[0])
            print("redis updated!")


def k_line_converter(soup, name, index):
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
        name=name,
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



