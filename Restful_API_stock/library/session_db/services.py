from library.models import session, db
from flask import current_app as app
import http.client
import json
import pandas as pd



def get_total_pages(symbol):
    page_info = {
        'HNX-INDEX': 229,
        'UPCOM-INDEX': 240,
        'VNINDEX': 289
    }
    return page_info.get(symbol, 0)

def up_session_db(floor):
    total_pages = get_total_pages(floor)
    for page_number in range(1, total_pages + 1):
        url = f"https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={floor}&PageIndex={page_number}&PageSize=19"
        headers = {'Accept': 'application/json'}
        conn = http.client.HTTPSConnection("s.cafef.vn")
        conn.request("GET", url, headers=headers)
        res = conn.getresponse()
        data = res.read()
        conn.close()

        if res.status == 200 and data:
            try:
                json_data = json.loads(data.decode('utf-8'))
                df = pd.json_normalize(json_data['Data'], record_path='Data')
                df['Ngay'] = pd.to_datetime(df['Ngay'], format='%d/%m/%Y', errors='coerce')
                with db.session.begin():
                    for _, row in df.iterrows():
                        new_session = session(
                            quarter=(row['Ngay'].month - 1) // 3 + 1,
                            day_of_week=row['Ngay'].weekday(),
                            day_of_month=row['Ngay'].day,
                            month=row['Ngay'].month,
                            year=row['Ngay'].year,
                            hour=0,
                            minute=0,
                            second=0,
                            volume=row.get('GiaTriKhopLenh', 0),
                            stock_space=floor  # Assuming floor is also stored
                        )
                        db.session.add(new_session)
                    db.session.commit()
            except Exception as e:
                app.logger.error(f"Error processing data for {floor} on page {page_number}: {str(e)}")
        else:
            app.logger.error(f"Failed to retrieve data for {floor} on page {page_number}. Status code: {res.status}")
