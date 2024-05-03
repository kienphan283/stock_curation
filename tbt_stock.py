import http.client
import json
import pandas as pd
from pandas import json_normalize

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

    # Read and decode the response to JSON
    data = res.read()
    json_data = json.loads(data.decode('utf-8'))  #  byte => chuỗi, json => python
    data_info = json_data['data'] 
        
    # Normalize the data and write to CSV
    df = json_normalize(data_info)

    #đổi tên
    df.rename(columns = {
        'companyName': 'name',
        'floor': 'stock_space',
        'listedDate': 'join_date',
        'delistedDate': 'leave_date'

    }, inplace = True)

    #chọn cột
    selectedColumns = df[['name', 'code', 'stock_space', 'join_date', 'leave_date']]

    selectedColumns.to_csv("tbt_stock.csv", mode='a', encoding='utf-8', index=False, sep=';')

    conn.close()




fetch_data()