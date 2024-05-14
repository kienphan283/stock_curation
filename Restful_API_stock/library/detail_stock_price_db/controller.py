from flask import Blueprint, jsonify
from detail_stock_price_db.services import insert_detail_stock_price_db

stock_price = Blueprint('detail_stock_price', __name__)
@stock_price.route('/detail_stock_price', methods=['GET'])
def stock_price():
    if insert_detail_stock_price_db():
        return jsonify({"message": "stock price inserted successfully"}), 200
    else:
        return jsonify({"error": "fail to insert stock price"}), 500