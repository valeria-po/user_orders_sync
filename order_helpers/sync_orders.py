# 1. pymongo - get, (select) from mongodb

# 2. if postgres == 0, load everything, else: load diff - set
import pandas as pd
from prod_db import get_pd_users_orders
from wh_db import get_wh_users_orders, write_to_db


def sync_db() -> str:
    """
    Syncs up users and orders from production db to warehouse db
    :return: DB state, can be either synced or how many new items were added
    """
    prod_items = get_pd_users_orders()
    wh_items = get_wh_users_orders()

    if wh_items.equals(prod_items):
        return "DBs are synced"
    else:
        # new_items = set(prod_items) ^ set(wh_items) find difference by using set on ids
        new_items = pd.concat([prod_items, wh_items]).drop_duplicates(keep=False)
        write_to_db(new_items)
        return f"Added {len(new_items)} new items"


sync_db()
