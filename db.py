import pandas as pd
import mysql.connector

def upLoadData():
    # Connect to MySQL
    try:
        connection = mysql.connector.connect(
            host="localhost", 
            user="root", 
            password="ngantran", 
            database="stock_lab.uit"
        )
        cursor = connection.cursor()


        #tạo bảng tbt_stock
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tbt_stock (
                        id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        name VARCHAR(1024) DEFAULT NULL,
                        code VARCHAR(16) NOT NULL UNIQUE,
                        country VARCHAR(4) NOT NULL DEFAULT 'VI',
                        stock_space VARCHAR(16) NOT NULL,
                        join_date datetime DEFAULT NULL,
                        leave_date datetime DEFAULT NULL,
                        init_price FLOAT NOT NULL DEFAULT 0,
                        updated_at datetime NOT NULL DEFAULT current_timestamp(),
                        created_at datetime NOT NULL DEFAULT current_timestamp(),
                        updated_by INT(11) NOT NULL DEFAULT 1,
                        created_by INT(11) NOT NULL DEFAULT 1,
                        category VARCHAR(1024) DEFAULT NULL,
                        isIndex tinyint(1) NOT NULL DEFAULT 0
                       )ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
                       """)
        
        #Tạo bảng session
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS session(
                       id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                       quarter INT(11) NOT NULL,
                       day_of_week INT(11) NOT NULL,
                       day_of_month INT(11) NOT NULL
                       month INT(11) NOT NULL,
                       year INT(11) NOT NULL,
                       hour INT(11) NOT NULL,
                       minute INT(11) NOT NULL,
                       second INT(11) NOT NULL,
                       volume FLOAT NOT NULL DEFAULT 0,
                       updated_at datetime NOT NULL DEFAULT current_timestamp(),
                       created_at datetime NOT NULL DEFAULT current_timestamp(),
                       updated_by INT(11) NOT NULL DEFAULT 1,
                       created_by INT(11) NOT NULL DEFAULT 1
                       )ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
                       """)
        
        #Tạo bảng detail_stock_price
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS detail_stock_price(
                       id INT(11) NOT NULL,
                        stock_id int(11) NOT NULL,
                        session_id int(11) NOT NULL,
                        open_price float NOT NULL,
                        close_price float NOT NULL,
                        high_price float NOT NULL,
                        low_price float NOT NULL,
                        volume float NOT NULL,
                        updated_at datetime NOT NULL DEFAULT current_timestamp(),
                        created_at datetime NOT NULL DEFAULT current_timestamp(),
                        updated_by int(11) NOT NULL DEFAULT 1,
                        created_by int(11) NOT NULL DEFAULT 1,
                        FOREIGN KEY (stock_id) REFERENCES tbt_stock(id),
                        FOREIGN KEY (session_id) REFERENCES session(id)
                       )ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
                       """)



       
        connection.commit()
    
    except mysql.connector.Error as e:
        print(f"An error occurred: {e}")
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

upLoadData()
