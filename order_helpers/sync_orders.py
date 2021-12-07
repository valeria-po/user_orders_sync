# 1. pymongo - get, (select) from mongodb

# 2. if postgres == 0, load everything, else: load diff - set

# 3. connect to postgres, psycopg - load by ids in set

# P.S. - requirements.txt, git init, tests.
import psycopg2
from pymongo import MongoClient

prod_client = MongoClient('localhost', 27017)
prod_db = prod_client.prod
user_col = prod_db.users
order_col = prod_db.orders


wh_client = psycopg2.connect(database="user_orders", user="postgres", password="root", port="5433")
cur = wh_client.cursor()
cur.execute("CREATE TABLE users_orders (id serial PRIMARY KEY, user_id varchar)")

wh_client.commit()


def sync_db() -> str:
    """
    Syncs up users and orders from production db to warehouse db
    :return: DB state, can be either synced or how many new items were added
    """
    prod_items = order_col
    wh_items = cur.execute("SELECT ids from user_orders")

    if prod_items == wh_items:
        return "DBs are synced"
    else:
        new_items = set(prod_items) ^ set(wh_items)
        cur.execute("INSERT into user_oders")
        return f"Added {len(new_items)} new items"
