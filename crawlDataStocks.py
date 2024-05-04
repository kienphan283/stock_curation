import http.client
import json
import pandas as pd
from pandas import json_normalize
import os #os: tương tác với hệ thống điều hành

# Đường dẫn đến thư mục lưu trữ các file CSV
csv_directory = r'C:\IT\STOCKS_LAB.UIT\exportData\allDataStocks'
# os.makedirs() để tạo một thư mục mới.
os.makedirs(csv_directory, exist_ok=True)  # Tạo thư mục nếu nó chưa tồn tại

#đọc file csv chứa các mã stocks
stock_data = pd.read_csv(r'C:\IT\STOCKS_LAB.UIT\exportData\tbt_stock')
#.unique(): Trích xuất các giá trị duy nhất từ cột
symbols = stock_data['code'].unique()


#Lặp qua từng mã Stocks:
for symbol in symbols:

    first_page = True


#w: lấy lại từ đầu, a: lấy data rồi viết vào cuối file bao gồm cả data cũ, r+: ghi vào bất cứ vị trí nào
    with open(os.path.join(csv_directory, f'{symbol}.csv'), 'w', encoding='utf-8', newline='') as f:

        # chạy từ trang 1 đến 289 để lấy data
        for page_number in range(1, 290):
            conn = http.client.HTTPSConnection("s.cafef.vn")
            headers = {
                'Referer': 'https://s.cafef.vn/du-lieu-doanh-nghiep.chn'
            }
            url = f"/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={symbol}&StartDate=&EndDate=&PageIndex={page_number}&PageSize=19"
            conn.request("GET", url, '', headers)
            res = conn.getresponse()
            data = res.read()
            
            if data:
                json_data = json.loads(data)
                
                # Kiểm tra nếu "Data" tồn tại trong dictionary JSON bên ngoài (=>phản hồi có chứa dữ liệu cần thiết).
                # Kiểm tra nếu "Data" tồn tại bên trong đối tượng "Data" (=> có một mảng dữ liệu muốn trích xuất).
                if 'Data' in json_data and 'Data' in json_data['Data']:
                    df = json_normalize(json_data['Data'], record_path='Data')
                 

                    if first_page:
                        df.to_csv(f, index=False, header=True)
                        first_page = False
                    else:
                        df.to_csv(f, index=False, header=False)
                else:
                    print(f"No data found on page {page_number}")
            else:
                print(f"No response from server on page {page_number}")
