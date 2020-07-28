from sqlitis.convert import to_sqla
from sqlalchemy import text, func
from sqlalchemy.sql import select, and_, or_
from sqlalchemy.orm import Query
from db import create_session, staff, products, customers, products, Staff, Product, Stock, Customer, Order, OrderItem
from ast import literal_eval

stmt = to_sqla(
    'select * from products where products.store = "Kenya"').replace(".c.", ".")
sess = create_session()
rel_lookup = {
    'staff': Staff,
    'products': Product,
    'stocks': Stock,
    'orders': Order,
    'order_items': OrderItem,
    'customers': Customer
}
exec(f'stmt = {stmt}')
q = sess.query(*[rel_lookup[str(table)] for table in stmt.froms]).add_columns(
    *[col.expression for col in stmt.columns]).from_statement(text('select * from products where products.store = \'Kenya\''))
print(q.all())
