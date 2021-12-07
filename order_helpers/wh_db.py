import psycopg2
from pandas import DataFrame

wh_client = psycopg2.connect(database="user_orders", user="postgres", password="root", port="5433")
cur = wh_client.cursor()


def get_wh_users_orders():
    # cur.execute("CREATE TABLE IF NOT EXISTS users_orders (id integer, user_id integer, first_name varchar, "
    #             "last_name varchar, merchant_id integer, phone_number integer, created_at_x varchar, "
    #             "updated_at_x varchar, created_at_y varchar, date_tz varchar, item_count varchar, order_id varchar, "
    #             "receive_method varchar, status varchar, store_id varchar, subtotal varchar, tax_percentage varchar, "
    #             "total varchar, total_discount varchar, total_gratuity varchar, "
    #             "total_tax varchar, updated_at_y varchar, fulfillment_date_tz varchar)")
    cur.execute("CREATE TABLE IF NOT EXISTS user_order_version_two "
                "(id varchar, user_id varchar, last_name varchar, order_id varchar)")
    all_user_orders = cur.execute("SELECT * FROM user_order_version_two")
    user_orders = DataFrame(all_user_orders)
    wh_client.commit()

    return user_orders


def write_to_db(prod_user_orders: DataFrame):
    for index, row in prod_user_orders.iterrows():
        cur.execute("INSERT INTO user_order_version_two values(%s, %s, %s, %s)",
                    (row["id"],
                     row["user_id"],
                     row["last_name"],
                     row["order_id"]))
    wh_client.commit()
