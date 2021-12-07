import pandas as pd
from pandas import DataFrame
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.prod
user_col = db.small_users
order_col = db.small_orders

def get_users():
    all_users = list(user_col.find())

    return all_users


def get_orders():
    all_orders = list(order_col.find())

    return all_orders


def get_pd_users_orders():
    """
    Function to extract all users and orders from production database and
    return them in a pandas Dataframe
    :return: Users and Orders Dataframe
    """
    users = list(user_col.find())
    orders = list(order_col.find())

    # users_df = DataFrame(users)
    # orders_df = DataFrame(orders)

    # users_orders = pd.merge(users_df, orders_df, left_on=['user_id'], right_on=['user_id'], how='left')
    # users_orders = users_df.join(orders_df, how="left", lsuffix="_user")

    # return users_orders

