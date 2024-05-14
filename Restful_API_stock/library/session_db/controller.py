from flask import Blueprint, jsonify
from session_db.services import up_session_db

session_bp = Blueprint('session_operations', __name__, url_prefix='/api/sessions')

@session_bp.route('/update', methods=['GET'])
def update_sessions():
    try:
        indices = ['HNX-INDEX', 'UPCOM-INDEX', 'VNINDEX']
        for index in indices:
            up_session_db(index)
        return jsonify({"message": "Sessions updated successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
