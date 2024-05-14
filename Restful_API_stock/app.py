from flask import Flask
from library.models import tbt_stock, session, detail_stock_price, db
from library.config import Config
from library.session_db.controller import session_bp
from library.tbt_stock_db.controller import tbt_stock_bp


if __name__=="__main__":
    app = Flask(__name__)
    app.config.from_object(Config)
    db.init_app(app)
    with app.app_context():
        db.create_all()  # Tạo các bảng cơ sở dữ liệu
    app.register_blueprint(session_bp, url_prefix='/session')
    app.register_blueprint(tbt_stock_bp, url_prefix='/stock')
    app.run(debug=True)