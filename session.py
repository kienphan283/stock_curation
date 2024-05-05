import http.client
import json
import pandas as pd
from pandas import json_normalize

# Hàm lấy tổng số trang tối đa dựa trên từ điển `page_info`
def get_total_pages(symbol):
    # Cập nhật số trang tối đa đã biết cho từng mã chứng khoán
    page_info = {
        'HNX-INDEX': 229,
        'UPCOM-INDEX': 240,  
        'VNINDEX': 289
    }
    return page_info.get(symbol, 0)

# Hàm tải dữ liệu cho từng mã chứng khoán
def session(floor):
    total_pages = get_total_pages(floor)
    first_page = True

    with open(f'{floor}.csv', 'w', encoding='utf-8', newline='') as f:
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
                        
                        # Định dạng lại cột 'GiaTriKhopLenh' và 'GtThoaThuan'
                        df['GiaTriKhopLenh'] = df['GiaTriKhopLenh'].apply(lambda x: f'{x:,.0f}')
                        df['GtThoaThuan'] = df['GtThoaThuan'].apply(lambda x: f'{x:,.0f}')
                        
                        # Định dạng ngày tháng trong cột 'Ngay' khi ghi vào CSV
                        df['Ngay'] = df['Ngay'].dt.strftime('%d/%m/%Y')
                        
                        # Ghi tên cột cho lần đầu tiên, sau đó ghi dữ liệu mà không có tiêu đề
                        if first_page:
                            df.to_csv(f, index=False, header=True)
                            first_page = False
                        else:
                            df.to_csv(f, index=False, header=False)
                    else:
                        print(f"Không tìm thấy dữ liệu ở trang {page_number} cho {floor}. JSON Response: {json_data}")
                except json.JSONDecodeError as e:
                    print(f"JSON Decode Error on page {page_number} cho {floor}: {e}")
            else:
                print(f"Không có phản hồi từ máy chủ ở trang {page_number} cho {floor}")

# Chạy hàm với các chỉ số chứng khoán mong muốn
session('HNX-INDEX')
session('UPCOM-INDEX')
session('VNINDEX')
