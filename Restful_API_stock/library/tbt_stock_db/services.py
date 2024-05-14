import http.client
import json
import pandas as pd
from pandas import json_normalize
import mysql.connector
from flask import current_app as app

def fetch_data():
    headers = {
        'Accept': 'application/json',
        'Referer': 'https://dstock.vndirect.com.vn/du-lieu-thi-truong/lich-su-gia'
    }
    conn = http.client.HTTPSConnection("finfo-api.vndirect.com.vn")
    url = "/v4/stocks?q=type:stock,ifc~floor:HOSE,HNX,UPCOM&size=9999"
    conn.request("GET", url, headers=headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()
    
    if res.status != 200:
        app.logger.error(f"Failed to fetch data, status code: {res.status}")
        return None
    
    try:
        json_data = json.loads(data.decode('utf-8'))
        data_info = json_data['data']
        df = json_normalize(data_info)
        df.rename(columns={'companyName': 'name', 'code': 'code', 'floor': 'stock_space', 'listedDate': 'join_date', 'delistedDate': 'leave_date'}, inplace=True)
        df = df.where(pd.notnull(df), None)
        return df
    except json.JSONDecodeError as e:
        app.logger.error(f"Failed to decode JSON: {e}")
        return None

def insert_data_to_db(df):
    try:
        connection = mysql.connector.connect(host="localhost", user="root", password="", database="stock_db")
        cursor = connection.cursor()
        sql = """
        INSERT INTO tbt_stock (name, code, stock_space, join_date, leave_date) 
        VALUES (%s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE name=VALUES(name), code=VALUES(code), stock_space=VALUES(stock_space), join_date=VALUES(join_date), leave_date=VALUES(leave_date);
        """
        for i, row in df.iterrows():
            cursor.execute(sql, (row['name'], row['code'], row['stock_space'], row['join_date'], row['leave_date']))
        
        connection.commit()
        return True  # Đánh dấu hoạt động thành công
    except mysql.connector.Error as e:
        app.logger.error(f"An error occurred: {e}")
        return False  # Đánh dấu hoạt động thất bại
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            app.logger.info("MySQL connection is closed")
