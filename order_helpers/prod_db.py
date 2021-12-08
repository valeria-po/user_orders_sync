from typing import List

from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.prod
user_col = db.users
order_col = db.orders


def get_users() -> List:
    """
    Function to retrieve all user rows from prod db
    :return:
    """
    all_users = list(user_col.find())

    return all_users


def get_orders() -> List:
    """
    Function to retrieve all order rows from prod db
    :return:
    """
    all_orders = list(order_col.find())

    return all_orders
