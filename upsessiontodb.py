<<<<<<< HEAD
import http.client
import json
import mysql.connector
import pandas as pd
from pandas import json_normalize
from datetime import datetime

# Hàm lấy tổng số trang tối đa dựa trên từ điển `page_info`
def get_total_pages(symbol):
    # Cập nhật số trang tối đa đã biết cho từng mã chứng khoán
    page_info = {
        'HNX-INDEX': 229,
        'UPCOM-INDEX': 240,
        'VNINDEX': 289
    }
    return page_info.get(symbol, 0)

# Hàm tải dữ liệu và chèn vào cơ sở dữ liệu
def session(floor):
    total_pages = get_total_pages(floor)
    
    try:
        # Kết nối đến cơ sở dữ liệu MySQL
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="stock_lab.uit"
        )
        cursor = connection.cursor()
        
        # Tạo bảng `session` nếu chưa tồn tại
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS session (
            id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
            quarter INT(11) NOT NULL,
            day_of_week INT(11) NOT NULL,
            day_of_month INT(11) NOT NULL,
            month INT(11) NOT NULL,
            year INT(11) NOT NULL,
            hour INT(11) NOT NULL,
            minute INT(11) NOT NULL,
            second INT(11) NOT NULL,
            volume FLOAT NOT NULL DEFAULT 0,
            updated_at DATETIME NOT NULL DEFAULT current_timestamp(),
            created_at DATETIME NOT NULL DEFAULT current_timestamp(),
            updated_by INT(11) NOT NULL DEFAULT 1,
            created_by INT(11) NOT NULL DEFAULT 1
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
        """)
        
        for page_number in range(1, total_pages + 1):
            conn = http.client.HTTPSConnection("s.cafef.vn")
            url = f"/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={floor}&StartDate=&EndDate=&PageIndex={page_number}&PageSize=19"
          
            # Đọc dữ liệu
            conn.request("GET", url)
            res = conn.getresponse()
            data = res.read()
            
            if data:
                try:
                    json_data = json.loads(data)
                    if 'Data' in json_data and 'Data' in json_data['Data']:
                        df = json_normalize(json_data['Data'], record_path='Data')
                        
                        # Chuyển đổi cột 'Ngay' sang định dạng ngày tháng
                        df['Ngay'] = pd.to_datetime(df['Ngay'], format='%d/%m/%Y', errors='coerce')
                        
                        # Duyệt qua các hàng của DataFrame và chèn vào bảng `session`
                        for _, row in df.iterrows():
                            date_obj = row['Ngay']
                            quarter = (date_obj.month - 1) // 3 + 1
                            day_of_week = date_obj.weekday()
                            day_of_month = date_obj.day
                            month = date_obj.month
                            year = date_obj.year
                            hour = 0
                            minute = 0
                            second = 0
                            volume = float(str(row['GiaTriKhopLenh']).replace(',', '')) if pd.notna(row['GiaTriKhopLenh']) else 0
                            
                            cursor.execute("""
                            INSERT INTO session (quarter, day_of_week, day_of_month, month, year, hour, minute, second, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (quarter, day_of_week, day_of_month, month, year, hour, minute, second, volume))
                        
                        connection.commit()
                    else:
                        print(f"Không tìm thấy dữ liệu ở trang {page_number} cho {floor}. JSON Response: {json_data}")
                except json.JSONDecodeError as e:
                    print(f"JSON Decode Error on page {page_number} cho {floor}: {e}")
            else:
                print(f"Không có phản hồi từ máy chủ ở trang {page_number} cho {floor}")
    except mysql.connector.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Chạy hàm với các chỉ số chứng khoán mong muốn
session('HNX-INDEX')
session('UPCOM-INDEX')
session('VNINDEX')
=======
import http.client
import json
import mysql.connector
import pandas as pd
from pandas import json_normalize
from datetime import datetime

# Hàm lấy tổng số trang tối đa dựa trên từ điển `page_info`
def get_total_pages(symbol):
    # Cập nhật số trang tối đa đã biết cho từng mã chứng khoán
    page_info = {
        'HNX-INDEX': 229,
        'UPCOM-INDEX': 240,
        'VNINDEX': 289
    }
    return page_info.get(symbol, 0)

# Hàm tải dữ liệu và chèn vào cơ sở dữ liệu
def session(floor):
    total_pages = get_total_pages(floor)
    
    try:
        # Kết nối đến cơ sở dữ liệu MySQL
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="stock_lab.uit"
        )
        cursor = connection.cursor()
        
        # Tạo bảng `session` nếu chưa tồn tại
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS session (
            id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
            quarter INT(11) NOT NULL,
            day_of_week INT(11) NOT NULL,
            day_of_month INT(11) NOT NULL,
            month INT(11) NOT NULL,
            year INT(11) NOT NULL,
            hour INT(11) NOT NULL,
            minute INT(11) NOT NULL,
            second INT(11) NOT NULL,
            volume FLOAT NOT NULL DEFAULT 0,
            updated_at DATETIME NOT NULL DEFAULT current_timestamp(),
            created_at DATETIME NOT NULL DEFAULT current_timestamp(),
            updated_by INT(11) NOT NULL DEFAULT 1,
            created_by INT(11) NOT NULL DEFAULT 1
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;
        """)
        
        for page_number in range(1, total_pages + 1):
            conn = http.client.HTTPSConnection("s.cafef.vn")
            url = f"/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={floor}&StartDate=&EndDate=&PageIndex={page_number}&PageSize=19"
          
            # Đọc dữ liệu
            conn.request("GET", url)
            res = conn.getresponse()
            data = res.read()
            
            if data:
                try:
                    json_data = json.loads(data)
                    if 'Data' in json_data and 'Data' in json_data['Data']:
                        df = json_normalize(json_data['Data'], record_path='Data')
                        
                        # Chuyển đổi cột 'Ngay' sang định dạng ngày tháng
                        df['Ngay'] = pd.to_datetime(df['Ngay'], format='%d/%m/%Y', errors='coerce')
                        
                        # Duyệt qua các hàng của DataFrame và chèn vào bảng `session`
                        for _, row in df.iterrows():
                            date_obj = row['Ngay']
                            quarter = (date_obj.month - 1) // 3 + 1
                            day_of_week = date_obj.weekday()
                            day_of_month = date_obj.day
                            month = date_obj.month
                            year = date_obj.year
                            hour = 0
                            minute = 0
                            second = 0
                            volume = float(str(row['GiaTriKhopLenh']).replace(',', '')) if pd.notna(row['GiaTriKhopLenh']) else 0
                            
                            cursor.execute("""
                            INSERT INTO session (quarter, day_of_week, day_of_month, month, year, hour, minute, second, volume)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (quarter, day_of_week, day_of_month, month, year, hour, minute, second, volume))
                        
                        connection.commit()
                    else:
                        print(f"Không tìm thấy dữ liệu ở trang {page_number} cho {floor}. JSON Response: {json_data}")
                except json.JSONDecodeError as e:
                    print(f"JSON Decode Error on page {page_number} cho {floor}: {e}")
            else:
                print(f"Không có phản hồi từ máy chủ ở trang {page_number} cho {floor}")
    except mysql.connector.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Chạy hàm với các chỉ số chứng khoán mong muốn
session('HNX-INDEX')
session('UPCOM-INDEX')
session('VNINDEX')
>>>>>>> 971b1d56b690af74e501f410584615af8fa1e0dd
