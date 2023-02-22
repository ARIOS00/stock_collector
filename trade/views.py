from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()


@scheduler.scheduled_job("interval", seconds=3)
def test():
    print("11111111")


scheduler.start()
