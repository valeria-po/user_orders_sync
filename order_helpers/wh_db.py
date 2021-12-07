import psycopg2

wh_client = psycopg2.connect(database="user_orders", user="postgres", password="root", port="5433")
cur = wh_client.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS user_order_version_three "
            "(id varchar, user_id varchar,first_name varchar, last_name varchar, order_id varchar)")
wh_client.commit()


def get_wh_users_orders():
    """
    View existing items in the warehouse database
    :return:
    """
    wh_users_orders = []
    # cur.execute("CREATE TABLE IF NOT EXISTS users_orders (id integer, user_id integer, first_name varchar, "
    #             "last_name varchar, merchant_id integer, phone_number integer, created_at_x varchar, "
    #             "updated_at_x varchar, created_at_y varchar, date_tz varchar, item_count varchar, order_id varchar, "
    #             "receive_method varchar, status varchar, store_id varchar, subtotal varchar, tax_percentage varchar, "
    #             "total varchar, total_discount varchar, total_gratuity varchar, "
    #             "total_tax varchar, updated_at_y varchar, fulfillment_date_tz varchar)")

    cur.execute("SELECT * FROM user_order_version_three")
    all_user_orders = cur.fetchall()
    wh_client.commit()
    for item in all_user_orders:
        dicted = {
            "id": item[0],
            "user_id": item[1],
            "order_id": item[2],
            "last_name": item[3],
            "first_name": item[4]
        }
        wh_users_orders.append(dicted)

    return wh_users_orders


def write_to_db(prod_user_orders):
    """
    Write to database the initial and consequently updated items
    :param prod_user_orders: The items with which the db is updated
    :return:
    """
    for item in prod_user_orders:
        cur.execute("INSERT INTO user_order_version_three values(%s, %s, %s, %s, %s)",
                    (item["id"],
                     item["user_id"],
                     item["order_id"],
                     item["first_name"],
                     item["last_name"]
                     ))

    wh_client.commit()
