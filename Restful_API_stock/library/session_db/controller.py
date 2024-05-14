from flask import Blueprint, jsonify
from session_db.services import up_session_db

session_bp = Blueprint('insert_session_info', __name__)

@session_bp.route('/session', methods=['GET'])
def insert_session():
    try:
        success = True
        # Thực hiện cập nhật cho từng chỉ số
        for index in ['HNX-INDEX', 'UPCOM-INDEX', 'VNINDEX']:
            up_session_db(index)  # Giả sử hàm này không cần trả về giá trị
        if success:
            return jsonify({"message": "session inserted successfully"}), 200
        else:
            return jsonify({"error": "fail to insert session"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500  # Bắt và trả về lỗi nếu có
