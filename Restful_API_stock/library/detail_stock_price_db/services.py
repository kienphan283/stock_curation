import http.client
import json
import pandas as pd
from pandas import json_normalize
import mysql.connector
from mysql.connector import Error
import os
from flask import current_app as app

# Cấu hình cơ sở dữ liệu MySQL
db_config = {
    'host': 'localhost',
    'database': 'stock_db',
    'user': 'root',
    'password': ''
}

# Hàm thiết lập kết nối với cơ sở dữ liệu MySQL
def create_db_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            app.logger.info("Connected to MySQL database")
        return connection
    except Error as e:
        app.logger.error(f"Error: {e}")
        return None

# Hàm lấy `stock_id` từ bảng `tbt_stock`
def get_stock_id(connection, symbol):
    cursor = connection.cursor()
    query = "SELECT id FROM tbt_stock WHERE code = %s"
    cursor.execute(query, (symbol,))
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result else None

# Hàm lấy hoặc chèn `session_id` từ bảng `session`
def get_or_insert_session_id(connection, session_id_str):
    cursor = connection.cursor()
    try:
        query = "SELECT id FROM session WHERE id = %s"
        cursor.execute(query, (session_id_str,))
        result = cursor.fetchone()
        if result:
            session_id = result[0]
        else:
            insert_query = "INSERT INTO session (id) VALUES (%s)"
            cursor.execute(insert_query, (session_id_str,))
            connection.commit()
            session_id = cursor.lastrowid
        return session_id
    except Error as e:
        app.logger.error(f"Error retrieving/inserting session_id for session_id {session_id_str}: {e}")
        return None
    finally:
        cursor.close()

# Hàm chèn dữ liệu vào bảng `detail_stock_price`
def insert_into_db(connection, data):
    cursor = connection.cursor()
    try:
        insert_query = """
            INSERT INTO detail_stock_price (
                stock_id, session_id, open_price, close_price, high_price, low_price,
                volume, updated_by, created_by
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        connection.commit()
    except Error as e:
        app.logger.error(f"Error inserting data into detail_stock_price: {e}")
    finally:
        cursor.close()
        
def insert_detail_stock_price_db():
    # Đọc các mã chứng khoán từ tệp CSV
    csv_directory = r'D:\Code\Git\stock_uit.lab\allDataStocks'
    os.makedirs(csv_directory, exist_ok=True)
    stock_data = pd.read_csv(r'D:\Code\Git\stock_uit.lab\AllNameStock.csv')
    symbols = stock_data['code'].unique()

    # Thiết lập kết nối cơ sở dữ liệu
    connection = create_db_connection()

    if connection is not None:
        for symbol in symbols:
            all_data = []

            # Lấy `stock_id` từ bảng `tbt_stock`
            stock_id = get_stock_id(connection, symbol)
            if stock_id is None:
                app.logger.error(f"No stock_id found for symbol {symbol}")
                continue

            for page_number in range(1, 290):
                conn = http.client.HTTPSConnection("s.cafef.vn")
                headers = {
                    'Referer': 'https://s.cafef.vn/du-lieu-doanh-nghiep.chn'
                }
                url = f"/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={symbol}&StartDate=&EndDate=&PageIndex={page_number}&PageSize=20"
                conn.request("GET", url, '', headers)
                res = conn.getresponse()
                data = res.read()

                if data:
                    json_data = json.loads(data)

                    if 'Data' in json_data and 'Data' in json_data['Data']:
                        df = json_normalize(json_data['Data'], record_path='Data')

                        # Chuyển đổi các hàng của DataFrame thành các tuple để chèn vào bảng
                        for _, row in df.iterrows():
                            session_id_str = row.get('Ngay', '')
                            session_id = get_or_insert_session_id(connection, session_id_str)

                            if session_id is not None:
                                all_data.append((
                                    stock_id,  # `stock_id` lấy từ bảng `tbt_stock`
                                    session_id,  # `session_id` lấy từ bảng `session`
                                    row.get('GiaMoCua', 0.0),
                                    row.get('GiaDongCua', 0.0),
                                    row.get('GiaCaoNhat', 0.0),
                                    row.get('GiaThapNhat', 0.0),
                                    row.get('KLThoaThuan', 0.0),
                                    row.get('KhoiLuongKhopLenh', 0.0),
                                    1,  # `updated_by` (cố định)
                                    1   # `created_by` (cố định)
                                ))

                    else:
                        app.logger.error(f"No data found on page {page_number} for {symbol}")
                else:
                    app.logger.error(f"No response from server on page {page_number} for {symbol}")

            # Chèn dữ liệu vào bảng `detail_stock_price`
            if all_data:
                insert_into_db(connection, all_data)

        connection.close()
    else:
        app.logger.info("Database connection could not be established.")