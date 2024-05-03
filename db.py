import pandas as pd
import mysql.connector

def upLoadData():
    # Đọc dữ liệu từ csv
    tbt_stock = pd.read_csv('C:/IT/STOCKS_LAB.UIT/exportData/tbt_stock.csv', delimiter=';')
    allNameStock = pd.read_csv('C:/IT/STOCKS_LAB.UIT/exportData/allNameStock.csv', delimiter=',')

    print(tbt_stock.columns)
    print(allNameStock.columns)
    # Ghép hai DataFrame dựa trên cột 'code'
    df = pd.merge(tbt_stock, allNameStock[['code', 'category']], on='code', how='left')

    # Chuyển NaN thành None (tương đương NULL trong SQL)
    df = df.where(pd.notnull(df), None)

    # Connect to MySQL
    try:
        connection = mysql.connector.connect(
            host="localhost", 
            user="root", 
            password="ngantran", 
            database="stock_lab.uit"
        )
        cursor = connection.cursor()

        # SQL with handling for duplicate key by updating existing records
        sql = """
        INSERT INTO tbt_stock (name, code, stock_space, join_date, leave_date, category) 
        VALUES (%s, %s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE 
        name=VALUES(name), stock_space=VALUES(stock_space), join_date=VALUES(join_date), leave_date=VALUES(leave_date), category=VALUES(category)
        """
        for i, row in df.iterrows():
            cursor.execute(sql, (row['name'], row['code'], row['stock_space'], row['join_date'], row['leave_date'], row['category']))

        connection.commit()
        print("Data uploaded successfully")
    except mysql.connector.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

upLoadData()
