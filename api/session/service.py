import http.client
import json
import pandas as pd
from pandas import json_normalize
from datetime import datetime, timedelta 
from .model import db, tbt_session
from api.config import SESSION_CAFEF_URL, SESSION_REFERER_URL
from sqlalchemy import func

# Hàm lấy tổng số trang tối đa
def get_total_pages(symbol):
    page_info = {"HNX-INDEX": 230, "UPCOM-INDEX": 184, "VNINDEX": 289}
    return page_info.get(symbol, 0)

# Hàm lấy ngày mới nhất có dữ liệu từ sql
def get_latest_date(stock_space):
    latest_date = db.session.query(
        func.max(
            func.cast(func.concat(
                tbt_session.year, '-',
                func.lpad(tbt_session.month, 2, '0'), '-',
                func.lpad(tbt_session.day_of_month, 2, '0')
            ), db.Date)
        ).label('latest_date')
    ).select_from(tbt_session).filter(
        tbt_session.stock_space == stock_space 
    ).scalar()

    return latest_date

# Hàm lấy dữ liệu lịch sử giá
def fetch_session_data(symbol, page_number, start_date=None, end_date=None):
    url = SESSION_CAFEF_URL.format(symbol=symbol, page_number=page_number, start_date=start_date, end_date=end_date)
    headers = {"Accept": "application/json", "Referer": SESSION_REFERER_URL}

    conn = http.client.HTTPSConnection("s.cafef.vn")
    conn.request("GET", url, headers=headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()

    if res.status != 200:
        print(f"status code: {res.status}")
        return None

    try:
        json_data = json.loads(data.decode("utf-8"))

        if "Data" in json_data and "Data" in json_data["Data"]:
            df = json_normalize(json_data["Data"], record_path="Data")
            df["Ngay"] = pd.to_datetime(df["Ngay"], format="%d/%m/%Y", errors="coerce")
            return df
        else:
            print(f" JSON Response: {json_data}")
            return None
    except json.JSONDecodeError as e:
        print(f"Error decode JSON: {e}")
        return None


def up_session_db(symbol):
    total_pages = get_total_pages(symbol)
    stock_space = {"HNX-INDEX": "HNX", "UPCOM-INDEX": "UPCOM", "VNINDEX": "HOSE"}.get(symbol, "")
    
    # Lấy latest_date từ bảng tbt_session
    latest_date = get_latest_date(stock_space)
    
    if latest_date:
        latest_date_str = latest_date.strftime("%Y-%m-%d")
        latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d")
        start_date = (latest_date + timedelta(days=1)).strftime("%d/%m/%Y")
        print(f"Updating data from {start_date} for {symbol}")
    else:
        start_date = None
        print(f"No existing data found, fetching all data for {symbol}")

    end_date = datetime.now().strftime("%d/%m/%Y")

    for page_number in range(1, total_pages + 1):
        df = fetch_session_data(symbol, page_number, start_date, end_date)
        if df is not None and not df.empty:
            data_added = False
            for index, row in df.iterrows():
                date = row["Ngay"]
                if start_date and date < datetime.strptime(start_date, "%d/%m/%Y"):
                    continue  # Bỏ qua dữ liệu cũ nếu đã có start_date

                quarter = (date.month - 1) // 3 + 1
                day_of_week = date.weekday()
                day_of_month = date.day
                month = date.month
                year = date.year
                
                volume = (
                    float(str(row["GiaTriKhopLenh"]).replace(",", ""))
                    if pd.notna(row["GiaTriKhopLenh"])
                    else 0
                )

                new_session = tbt_session(
                    quarter=quarter,
                    day_of_week=day_of_week,
                    day_of_month=day_of_month,
                    month=month,
                    year=year,
                    volume=volume,
                    updated_at=datetime.utcnow(),
                    created_at=datetime.utcnow(),
                    updated_by=1,
                    created_by=1,
                    stock_space=stock_space
                )
                db.session.add(new_session)
                data_added = True

            if not data_added:
                break
    db.session.commit()
    print(f"Data for {symbol} was successfully loaded")
