from typing import List

from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.prod
user_col = db.small_users
order_col = db.small_orders


def get_users() -> List:
    all_users = list(user_col.find())

    return all_users


def get_orders() -> List:
    all_orders = list(order_col.find())

    return all_orders
