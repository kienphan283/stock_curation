from flask import Blueprint, jsonify
from tbt_stock_db.services import fetch_data, insert_data_to_db

tbt_stock_bp = Blueprint('insert_company_info', __name__)

@tbt_stock_bp.route('/stock_name', methods=['GET'])
def insert_tbt_stock():
    df = fetch_data()
    if df is not None and not df.empty:
        success = insert_data_to_db(df)
        if success:
            return jsonify({"message": "Stock data updated successfully"}), 200
        else:
            return jsonify({"error": "Fail to insert data"}), 500
    else:
        return jsonify({"error": "Fail to fetch data"}), 500
