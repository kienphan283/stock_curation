import http.client
import json
import pandas as pd
from pandas import json_normalize

first_page = True

with open('VNINDEX.csv', 'w', encoding='utf-8', newline='') as f:
    for page_number in range(1, 290):
        conn = http.client.HTTPSConnection("s.cafef.vn")
        headers = {
            'Referer': 'https://s.cafef.vn/du-lieu-doanh-nghiep.chn'
        }
        url = f"/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol=VNINDEX&StartDate=&EndDate=&PageIndex={page_number}&PageSize=19"
        conn.request("GET", url, '', headers)
        res = conn.getresponse()
        data = res.read()
        
        if data:
            json_data = json.loads(data)
            # Đảm bảo rằng 'Data' key tồn tại trong json_data và là một dictionary có chứa danh sách ở key 'Data'
            if 'Data' in json_data and 'Data' in json_data['Data']:
                df = json_normalize(json_data['Data'], record_path='Data')
                # Chuyển đổi cột 'Ngay' sang định dạng ngày tháng
                df['Ngay'] = pd.to_datetime(df['Ngay'], format='%d/%m/%Y', errors='coerce')
                
                # Định dạng lại cột 'GiaTriKhopLenh' và 'GtThoaThuan'
                df['GiaTriKhopLenh'] = df['GiaTriKhopLenh'].apply(lambda x: f'{x:,.0f}')
                df['GtThoaThuan'] = df['GtThoaThuan'].apply(lambda x: f'{x:,.0f}')
                
                # Định dạng ngày tháng trong cột 'Ngay' khi ghi vào CSV
                df['Ngay'] = df['Ngay'].dt.strftime('%d/%m/%Y')#.strftime(): chuyển đổi đối tượng thời gian thành một chuỗi theo định dạng đã cho.
                
                # Ghi tên cột cho lần ghi đầu tiên, sau đó ghi dữ liệu mà không ghi tiêu đề
                if first_page:
                    df.to_csv(f, index=False, header=True)
                    first_page = False
                else:
                    df.to_csv(f, index=False, header=False)
            else:
                print(f"No data found on page {page_number}") 
        else:
            print(f"No response from server on page {page_number}")
