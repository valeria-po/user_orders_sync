from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

from order_helpers.sync_orders import sync_db

app = Flask(__name__)

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(sync_db, 'interval', minutes=1)
scheduler.start()


@app.route('/')
def hello_world():
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
