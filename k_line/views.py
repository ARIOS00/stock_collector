import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from k_line.models import KLine
from datetime import date


from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()

# mock browser headers
headers = {
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
}

@scheduler.scheduled_job("interval", seconds=3)
def update_kline():

    # robot = RobotFileParser()
    # robot.set_url("https://finance.yahoo.com/robots.txt")
    # robot.read()
    # if robot.can_fetch('*', "https://finance.yahoo.com/quote/GE/history?p=GE"):


    # Create a new KLine object
    kline = KLine(
        name='ABC',
        k_date=date.today(),
        close=10.0,
        volume=1000.0,
        open=12.0,
        high=15.0,
        low=9.0,
    )

    # Save the object to the database

    print(kline.save())


scheduler.start()


# Create your views here.
# def hello(request):

#     return HttpResponse("Hello World!")
