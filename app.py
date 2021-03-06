from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

from order_helpers.sync_orders import sync_db

app = Flask(__name__)

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(sync_db, 'interval', minutes=5)
scheduler.start()


@app.route('/')
def wh_db():
    return "Warehouse db containing Users and Orders"


if __name__ == '__main__':
    app.run()
