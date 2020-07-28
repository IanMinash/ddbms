import json

from flask import Flask, request, abort, jsonify, make_response
from sqlalchemy import text
from .db import create_session, Staff, Product, Order, OrderItem, Customer, Stock

app = Flask(__name__)


def _build_cors_prelight_response():
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "*")
    response.headers.add("Access-Control-Allow-Methods", "*")
    return response


def _corsify_actual_response(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response


@app.route('/select', methods=["POST", "OPTIONS"])
def select():
    if request.method == 'OPTIONS':
        return _build_cors_prelight_response()
    if request.method == 'POST':
        sess = create_session()
        raw_stmt = request.form['sql']
        sql_stmt = text(raw_stmt)
        try:
            if "staff" in raw_stmt:
                res = sess.query(Staff).from_statement(sql_stmt).all()
            elif "products" in raw_stmt:
                res = sess.query(Product).from_statement(sql_stmt).all()
            elif "orders" in raw_stmt:
                res = sess.query(Order).from_statement(sql_stmt).all()
            elif "order_items" in raw_stmt:
                res = sess.query(OrderItem).from_statement(sql_stmt).all()
            elif "stocks" in raw_stmt:
                res = sess.query(Stock).from_statement(sql_stmt).all()
            elif "customers" in raw_stmt:
                res = sess.query(Customer).from_statement(sql_stmt).all()
            res = [obj.__dict__ for obj in res]
            for obj in res:
                obj['id'] = str(obj['id'])
                obj.pop('_sa_instance_state')
            sess.close()
            return _corsify_actual_response(jsonify(res))
        except Exception as e:
            sess.close()
            return _corsify_actual_response(make_response(jsonify({'status': e}), 400))


@app.route('/insert', methods=["POST", "OPTIONS"])
def insert():
    if request.method == 'OPTIONS':
        return _build_cors_prelight_response()
    if request.method == 'POST':
        sess = create_session()
        staff = Staff(request.form.get("first_name"), request.form.get("last_name"), request.form.get(
            "email"), request.form.get("active"), request.form.get("store"))
        sess.add(staff)
        try:
            sess.commit()
            sess.close()
            return _corsify_actual_response(make_response(jsonify({'status': 'ok'}), 201))
        except Exception as e:
            sess.close()
            return _corsify_actual_response(make_response(jsonify({'status': e}), 400))
