from typing import List

import psycopg2

wh_client = psycopg2.connect(database="user_orders", user="postgres", password="root", port="5433")
cur = wh_client.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS wh_users_orders (id integer, user_id integer, first_name varchar, "
            "last_name varchar, merchant_id integer, phone_number integer, users_created_at varchar, "
            "users_updated_at varchar, orders_created_at varchar, date_tz varchar, item_count varchar, order_id varchar, "
            "receive_method varchar, status varchar, store_id varchar, subtotal varchar, tax_percentage varchar, "
            "total varchar, total_discount varchar, total_gratuity varchar, "
            "total_tax varchar, orders_updated_at varchar, fulfillment_date_tz varchar)")
wh_client.commit()


def get_wh_users_orders() -> List:
    """
    View existing items in the warehouse database
    :return:
    """
    wh_users_orders = []

    cur.execute("SELECT * FROM wh_users_orders")
    all_user_orders = cur.fetchall()
    wh_client.commit()
    for item in all_user_orders:
        user_orders = {
            "id": item[0],
            "user_id": item[1],
            "first_name": item[2],
            "last_name": item[3],
            "merchant_id": item[4],
            "phone_number": item[5],
            "users_created_at": item[6],
            "users_updated_at": item[7],
            "id_order": item[8],
            "orders_created_at": item[9],
            "date_tz": item[10],
            "item_count": item[11],
            "order_id": item[12],
            "receive_method": item[13],
            "status": item[14],
            "store_id": item[15],
            "subtotal": item[16],
            "tax_percentage": item[17],
            "total": item[18],
            "total_discount": item[19],
            "total_gratuity": item[20],
            "total_tax": item[21],
            "orders_updated_at": item[22],
            "fulfillment_date_tz": item[23],
        }
        wh_users_orders.append(user_orders)

    return wh_users_orders


def write_to_db(prod_user_orders: List):
    """
    Write to database the initial and consequently updated items
    :param prod_user_orders: The items with which the db is updated
    :return:
    """
    for item in prod_user_orders:
        cur.execute("INSERT INTO wh_users_orders values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, "
                    "%s, %s,%s, %s, %s, %s, %s, %s)",
                    (item["id"],
                     item["user_id"],
                     item["first_name"],
                     item["last_name"],
                     item["merchant_id"],
                     item["phone_number"],
                     item["users_created_at"],
                     item["users_updated_at"],
                     item["id_order"],
                     item["orders_created_at"],
                     item["date_tz"],
                     item["item_count"],
                     item["order_id"],
                     item["receive_method"],
                     item["status"],
                     item["store_id"],
                     item["subtotal"],
                     item["tax_percentage"],
                     item["total"],
                     item["total_discount"],
                     item["total_gratuity"],
                     item["total_tax"],
                     item["orders_updated_at"],
                     item["fulfillment_date_tz"]
                     ))

    wh_client.commit()
