import http.client
import json
import mysql.connector
import pandas as pd
from pandas import json_normalize
from flask import current_app as app

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
def up_session_db(floor):
    total_pages = get_total_pages(floor)
    
    try:
        # Kết nối đến cơ sở dữ liệu MySQL
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="stock_db"
        )
        cursor = connection.cursor()
        
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
                        app.logger.error(f"Không tìm thấy dữ liệu ở trang {page_number} cho {floor}. JSON Response: {json_data}")
                except json.JSONDecodeError as e:
                    app.logger.error(f"JSON Decode Error on page {page_number} cho {floor}: {e}")
            else:
                app.logger.error(f"Không có phản hồi từ máy chủ ở trang {page_number} cho {floor}")
    except mysql.connector.Error as e:
        app.logger.error(f"An error occurred: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            app.logger.info("MySQL connection is closed")

