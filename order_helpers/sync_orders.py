from prod_db import get_users, get_orders
from wh_db import get_wh_users_orders, write_to_db


def sync_db():
    """
    Syncs up users and orders from production db to warehouse db
    :return: DB state, can be either synced or how many new items were added
    """

    db_prod_items = []
    prod_items = get_users() + get_orders()
    wh_items = get_wh_users_orders()

    for item in prod_items:
        users_orders_unified = {
            "id": item.get("id", ),
            "user_id": item.get("user_id", ),
            "first_name": item.get("first_name", ),
            "last_name": item.get("last_name", ),
            "merchant_id": item.get("merchant_id", ),
            "phone_number": item.get("phone_number", ),
            "users_created_at": item.get("users_created_at", ),
            "users_updated_at": item.get("users_updated_at", ),
            "id_order": item.get("id_order", ),
            "orders_created_at": item.get("orders_created_at", ),
            "date_tz": item.get("date_tz", ),
            "item_count": item.get("item_count", ),
            "order_id": item.get("order_id", ),
            "receive_method": item.get("receive_method", ),
            "status": item.get("status", ),
            "store_id": item.get("store_id", ),
            "subtotal": item.get("subtotal", ),
            "tax_percentage": item.get("tax_percentage", ),
            "total": item.get("total", ),
            "total_discount": item.get("total_discount", ),
            "total_gratuity": item.get("total_gratuity", ),
            "total_tax": item.get("total_tax", ),
            "orders_updated_at": item.get("orders_updated_at", ),
            "fulfillment_date_tz": item.get("fulfillment_date_tz", ),
        }
        db_prod_items.append(users_orders_unified)

    wh_user_ids = [user["user_id"] for user in wh_items if user["user_id"] is not None]
    prod_user_ids = [user["user_id"] for user in db_prod_items if user["user_id"] is not None]

    wh_order_ids = [order["order_id"] for order in wh_items if order["order_id"] is not None]
    prod_order_ids = [order["order_id"] for order in db_prod_items if order["order_id"] is not None]

    if len(db_prod_items) == len(wh_items):
        print("DBs are synced")
    else:
        added_items = []
        new_user_ids = set(wh_user_ids) ^ set(prod_user_ids)
        new_order_ids = set(wh_order_ids) ^ set(prod_order_ids)

        for item in db_prod_items:
            if item["user_id"] in new_user_ids:
                added_items.append(item)
            elif item["order_id"] in new_order_ids:
                added_items.append(item)
        write_to_db(added_items)
        print(f"Added {len(added_items)} new items")


sync_db()
