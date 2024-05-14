from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func

db = SQLAlchemy()

class tbt_stock(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(1024), nullable=False)
    code = db.Column(db.String(16), nullable=False, unique=True)
    country = db.Column(db.String(4), nullable=False, default='VI')
    stock_space = db.Column(db.String(16), nullable=False)
    join_date = db.Column(db.DateTime, nullable=True)
    leave_date = db.Column(db.DateTime, nullable=True)
    init_price = db.Column(db.Float, nullable=False, default=0)
    updated_at = db.Column(db.DateTime, nullable=False, default=func.current_timestamp())
    created_at = db.Column(db.DateTime, nullable=False, default=func.current_timestamp())
    updated_by = db.Column(db.Integer, nullable=False, default=1)
    created_by = db.Column(db.Integer, nullable=False, default=1)
    category = db.Column(db.String(1024), nullable=True)
    isIndex = db.Column(db.SmallInteger, nullable=False, default=0)

    def __init__(self, name, code, country, join_date, leave_date, init_price, updated_at, created_at, updated_by, created_by, category, isIndex):
        self.name = name
        self.code = code
        self.country = country
        self.join_date = join_date
        self.leave_date = leave_date
        self.init_price = init_price
        self.updated_at = updated_at
        self.created_at = created_at
        self.updated_by = updated_by
        self.created_by = created_by
        self.category = category
        self.isIndex = isIndex

class session(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    quarter = db.Column(db.Integer, nullable=False)
    day_of_week = db.Column(db.Integer, nullable=False)
    day_of_month = db.Column(db.Integer, nullable=False)
    month = db.Column(db.Integer, nullable=False)
    year = db.Column(db.Integer, nullable=False)
    hour = db.Column(db.Integer, nullable=False)
    minute = db.Column(db.Integer, nullable=False)
    second = db.Column(db.Integer, nullable=False)
    volume = db.Column(db.Float, nullable=False, default=0)
    updated_at = db.Column(db.DateTime, nullable=False, default=func.current_timestamp())
    created_at = db.Column(db.DateTime, nullable=False, default=func.current_timestamp())
    updated_by = db.Column(db.Integer, nullable=False, default=1)
    created_by = db.Column(db.Integer, nullable=False, default=1)
    stock_space = db.Column(db.String(16), nullable=False)

    def __init__(self, quarter, day_of_week, day_of_month, month, year, hour, minute, second, volume, updated_at, created_at, updated_by, created_by, stock_space):
        self.quarter = quarter
        self.day_of_week = day_of_week
        self.day_of_month = day_of_month
        self.month = month
        self.year = year
        self.hour = hour
        self.minute = minute
        self.second = second
        self.volume = volume
        self.updated_at = updated_at
        self.created_at = created_at
        self.updated_by = updated_by
        self.created_by = created_by
        self.stock_space = stock_space

class detail_stock_price(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    stock_id = db.Column(db.Integer, db.ForeignKey('tbt_stock.id'), nullable=False)
    session_id = db.Column(db.Integer, db.ForeignKey('session.id'), nullable=False)
    open_price = db.Column(db.Float, nullable=False)
    close_price = db.Column(db.Float, nullable=False)
    high_price = db.Column(db.Float, nullable=False)
    low_price = db.Column(db.Float, nullable=False)
    volume = db.Column(db.Float, nullable=False)
    updated_at = db.Column(db.DateTime, nullable=False, default=func.current_timestamp())
    created_at = db.Column(db.DateTime, nullable=False, default=func.current_timestamp())
    updated_by = db.Column(db.Integer, nullable=False, default=1)
    created_by = db.Column(db.Integer, nullable=False, default=1)

    def __init__(self, stock_id, session_id, open_price, close_price, high_price, low_price, volume, updated_at, created_at, updated_by, created_by):
        self.stock_id = stock_id
        self.session_id = session_id
        self.open_price = open_price
        self.close_price = close_price
        self.high_price = high_price
        self.low_price = low_price
        self.volume = volume
        self.updated_at = updated_at
        self.created_at = created_at
        self.updated_by = updated_by
        self.created_by = created_by