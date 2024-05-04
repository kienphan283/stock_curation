import http.client
import json
import pandas as pd
from pandas import json_normalize
import mysql.connector

def fetch_data():
    

    headers = {
        'Accept': 'application/json',
        'Referer': 'https://dstock.vndirect.com.vn/du-lieu-thi-truong/lich-su-gia'
    }

    conn = http.client.HTTPSConnection("finfo-api.vndirect.com.vn")
    url = "/v4/stocks?q=type:stock,ifc~floor:HOSE,HNX,UPCOM&size=9999"

    # Make the request with headers
    conn.request("GET", url, headers=headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()
    # Read and decode the response to JSON
    if res.status != 200:
        print(f"Failed to fetch data, status code: {res.status}")
        return None
    
    try:
        json_data = json.loads(data.decode('utf-8'))  #  byte => chuỗi, json => python
        data_info = json_data['data'] 
     
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
        return None
        
    # Normalize the data 
    df = json_normalize(data_info)
    df.rename(columns={'companyName': 'name', 'code': 'code', 'floor': 'stock_space', 'listedDate': 'join_date', 'delistedDate': 'leave_date'}, inplace=True)
    # Chuyển NaN thành None (tương đương NULL trong SQL)
    df = df.where(pd.notnull(df), None)

    try:
        #Connect to SQL
        connection = mysql.connector.connect(
            host="localhost", 
            user="root", 
            password="ngantran", 
            database="stock_lab.uit"
        )
        cursor = connection.cursor() #cau  lenh de  thuc thi sql
        sql = """
            INSERT INTO tbt_stock (name, code, stock_space, join_date, leave_date) 
            VALUES (%s, %s, %s, %s, %s) 
            ON DUPLICATE KEY UPDATE 
            name=VALUES(name), code=VALUES(code), stock_space=VALUES(stock_space), join_date=VALUES(join_date), leave_date=VALUES(leave_date);
            """


        for i, row in df.iterrows():
            cursor.execute(sql, (row['name'], row['code'], row['stock_space'], row['join_date'], row['leave_date']))
        
        #xác nhận các thay đổi 
        connection.commit()

    except mysql.connector.Error as e:
        print(f"An error occurred: {e}")

    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

fetch_data()