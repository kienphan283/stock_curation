import http.client
import json
import pandas as pd
from flask import current_app as app
from library.models import db, tbt_stock
from sqlalchemy.exc import SQLAlchemyError

def fetch_data():
    headers = {
        'Accept': 'application/json',
        'Referer': 'https://dstock.vndirect.com.vn/du-lieu-thi-truong/lich-su-gia'
    }
    conn = http.client.HTTPSConnection("finfo-api.vndirect.com.vn")
    url = "/v4/stocks?q=type:stock,ifc~floor:HOSE,HNX,UPCOM&size=9999"
    conn.request("GET", url, headers=headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()
    
    if res.status != 200:
        app.logger.error(f"Failed to fetch data, status code: {res.status}")
        return None
    
    try:
        json_data = json.loads(data.decode('utf-8'))
        data_info = json_data['data']
        df = pd.json_normalize(data_info)
        df.rename(columns={'companyName': 'name', 'code': 'code', 'floor': 'stock_space', 'listedDate': 'join_date', 'delistedDate': 'leave_date'}, inplace=True)
        df = df.where(pd.notnull(df), None)
        return df
    except json.JSONDecodeError as e:
        app.logger.error(f"Failed to decode JSON: {e}")
        return None

def insert_data_to_db(df):
    try:
        for _, row in df.iterrows():
            stock = tbt_stock(
                name=row['name'],
                code=row['code'],
                stock_space=row['stock_space'],
                join_date=row['join_date'],
                leave_date=row['leave_date']
            )
            db.session.add(stock)
        db.session.commit()
        return True
    except SQLAlchemyError as e:
        app.logger.error(f"SQLAlchemy Error: {e}")
        db.session.rollback()
        return False
