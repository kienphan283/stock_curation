import http.client
import json
import pandas as pd
from pandas import json_normalize

first_page = True  # Biến để kiểm tra xem có phải là lần ghi đầu tiên không

# Ánh xạ từ TradeCenterId sang mã sàn giao dịch
trade_center_mapping = {
    1: "HOSE",
    2: "HNX",
    9: "UPCOM",
    8: "OTC"
}

with open('allNameStock.csv', 'w', encoding='utf-8', newline= '') as f:
    for page_number in range(1, 84):
        conn = http.client.HTTPSConnection("s.cafef.vn")
        headers = {
            'Referer': 'https://s.cafef.vn/du-lieu-doanh-nghiep.chn'
        }
        url = f"/ajax/pagenew/databusiness/congtyniemyet.ashx?centerid=0&skip={(page_number - 1) * 20}&take=20&major=0"
        conn.request("GET", url, '', headers)
        res = conn.getresponse()
        data = res.read()
        json_data = json.loads(data)

        df = json_normalize(json_data, record_path='Data')

        # Thay đổi cột TradeCenterId bằng mã sàn tương ứng sử dụng từ điển
        df['TradeCenterId'] = df['TradeCenterId'].map(trade_center_mapping)

        #Đổi tên cột
        df.rename(columns = {'TradeCenterId': 'stock_space',
                            'Symbol': 'code',
                            'CompanyName': 'name',
                            'CategoryName': 'category'}, inplace = True) #inplace: xác định thay đổi trực tiếp hay không
        
        selectedColumns = df[['stock_space', 'code', 'name', 'category']]

        # Ghi tên cột cho lần ghi đầu tiên, sau đó ghi dữ liệu mà không ghi tiêu đề
        if first_page:
            selectedColumns.to_csv(f, index=False, header=True)
            first_page = False
        else:
            selectedColumns.to_csv(f, index=False, header=False)