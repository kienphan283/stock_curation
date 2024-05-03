import http.client
import json
import pandas as pd

def fetch_data(TradeCenter='', startDate=''):
    conn = http.client.HTTPSConnection("s.cafef.vn")
    # Properly format the URL and headers
    headers = {'Accept': 'application/json'}
    url = f"/Ajax/PageNew/DataGDNN/LichSuGia.ashx?TradeCenter={TradeCenter}&Date={startDate}"
    conn.request("GET", url, headers=headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()

    # Check the response status and handle non-success responses
    if response.status != 200:
        print(f"Failed to fetch data, status code: {response.status}")
        return None

    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
        return None

def process_data(json_data):
    df = pd.DataFrame(json_data)
    if not df.empty:
        for col in df.columns:
            if isinstance(df[col].iloc[0], dict):
                sub_df = pd.json_normalize(df[col])
                sub_df.columns = [f"{subcol}" for subcol in sub_df.columns]
                df = pd.concat([sub_df], axis=1)
    else:
        print("Dataframe is empty after loading JSON data.")
    return df

def save_to_csv(df, filename):
    if not df.empty:
        with open(filename, 'a', encoding='utf-8') as file:
            df.to_csv(file, index=False, sep=';')
    else:
        print("No data to save to CSV.")

json_data = fetch_data('UPCOM', "04/25/2024")
if json_data and json_data.get('Data'):
    df = process_data(json_data['Data'])
    if not df.empty:
        constant_columns = [col for col in df.columns if df[col].nunique() == 1]
        constant_df = df[constant_columns].drop_duplicates()
        df.drop(columns=constant_columns, inplace=True)
        save_to_csv(df, 'stock1Day.csv')
        save_to_csv(constant_df, 'constant_data1.csv')
    else:
        print("No valid data entries found in the fetched data.")
else:
    print("No data found for the given date or data structure is unexpected.")