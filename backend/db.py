import datetime
import os
import uuid
import random

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy import Table
from sqlalchemy import event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import backref
from sqlalchemy.sql import operators
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import BooleanClauseList, BinaryExpression
from sqlalchemy.types import TypeDecorator, CHAR
from sqlalchemy.dialects.postgresql import UUID
from faker import Faker


fake = Faker()


class GUID(TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.

    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value


postgres = create_engine(
    f"postgresql://{os.environ.get('POSTGRES_USER')}:{os.environ.get('POSTGRES_PASS')}@{os.environ.get('POSTGRES_HOST')}/{os.environ.get('DB_NAME')}")
sql_server = create_engine(
    f"mssql+pyodbc://{os.environ.get('SQLSERVER_USER')}:{os.environ.get('SQLSERVER_PASS')}@{os.environ.get('SQLSERVER_HOST')}/{os.environ.get('DB_NAME')}?driver=SQL+Server+Native+Client+11.0")
sqlite = create_engine("sqlite:///sqlite.db")

# create session function.  this binds the shard ids
# to databases within a ShardedSession and returns it.
create_session = sessionmaker(class_=ShardedSession)

create_session.configure(
    shards={
        "postgres": postgres,
        "sql_server": sql_server,
        "sqlite": sqlite,
    }
)


# mappings and tables
Base = declarative_base()


# table setup.


# Staff table
class Staff(Base):
    __tablename__ = "staff"

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    first_name = Column(String(30), nullable=False)
    last_name = Column(String(30))
    email = Column(String(250))
    active = Column(Boolean)
    store = Column(String(30))

    def __init__(self, first_name: str, last_name, email: str, active: bool, store: str):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.active = active
        self.store = store


staff = Staff


# Customer table
class Customer(Base):
    __tablename__ = 'customers'

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    first_name = Column(String(30), nullable=False)
    last_name = Column(String(30))
    email = Column(String(250))
    city = Column(String(50))
    store = Column(String(30))

    def __init__(self, first_name, last_name, email, city, store):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.city = city
        self.store = store


customers = Customer


# Order table
class Order(Base):
    __tablename__ = 'orders'

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    customer_id = Column(GUID, ForeignKey('customers.id'))
    order_date = Column(DateTime, default=datetime.datetime.now)
    staff_id = Column(GUID, ForeignKey('staff.id'))
    store = Column(String(30))
    order_status = Column(Boolean)

    customer = relationship("Customer", backref=backref(
        "orders", cascade="all, delete-orphan"))
    staff = relationship("Staff", backref="orders_attended")

    def __init__(self, customer, staff, store, order_status, date=None):
        self.customer_id = customer.id
        self.staff_id = staff.id
        self.store = store
        self.order_status = order_status
        if date:
            self.order_date = date


orders = Order


# OrderItem table
class OrderItem(Base):
    __tablename__ = 'order_items'

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    order_id = Column(GUID, ForeignKey('orders.id'))
    product_id = Column(GUID, ForeignKey('products.id'))
    quantity = Column(Integer)
    store = Column(String(30))

    order = relationship("Order", backref="order_items")
    product = relationship("Product")

    def __init__(self, order, item, quantity):
        self.order = order
        self.order_id = order.id
        self.product = item
        self.product_id = item.id
        self.quantity = quantity
        self.store = self.order.store


order_items = OrderItem

# @event.listens_for(OrderItem, 'after_insert')
# def reduce_stock(mapper, connection, target):
#     product = target.product
#     stock = [stock for stock in filter(
#         lambda x: x.store == target.store, product.stock)][0]
#     stock.quantity -= target.quantity


# Product table
class Product(Base):
    __tablename__ = 'products'

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    product_name = Column(String(250))
    list_price = Column(Integer)
    store = Column(String(30))

    def __init__(self, product_name, list_price, store):
        self.product_name = product_name
        self.list_price = list_price
        self.store = store


products = Product


# Stock table
class Stock(Base):
    __tablename__ = 'stocks'

    id = Column(GUID, primary_key=True, default=uuid.uuid4)
    product_id = Column(GUID, ForeignKey('products.id'))
    store = Column(String(30))
    quantity = Column(Integer)

    product = relationship("Product", backref=backref(
        "stock", cascade="all, delete-orphan"))

    def __init__(self, product: Product, list_price):
        self.product_id = product.id
        self.list_price = list_price
        self.store = product.store


stocks = Stock

# create tables
for db in (sql_server, postgres, sqlite):
    Base.metadata.create_all(db)


# we'll use a straight mapping of a particular set of "country"
# attributes to shard id.
shard_lookup = {
    "Kenya": "postgres",
    "Uganda": "sql_server",
    "Tanzania": "sqlite",
}


def shard_chooser(mapper, instance, clause=None):
    """shard chooser.

    looks at the given instance and returns a shard id

    """
    if clause is not None:
        if isinstance(clause._whereclause, BooleanClauseList):
            for c in clause._whereclause.clauses:
                if isinstance(c, BinaryExpression) and c.left.description == 'store':
                    return shard_lookup[c.right.effective_value]
        elif isinstance(clause._whereclause, BinaryExpression) and clause.left.description == 'store':
            return shard_lookup[clause.right.effective_value]
    return shard_lookup[instance.store]


def id_chooser(query, ident):
    """id chooser.

    given a primary key, returns a list of shards
    to search.

    """
    if query.lazy_loaded_from:
        # if we are in a lazy load, we can look at the parent object
        # and limit our search to that same shard, assuming that's how we've
        # set things up.
        return [query.lazy_loaded_from.identity_token]
    else:
        return ["postgres", "sql_server", "sqlite"]


def query_chooser(query):
    """query chooser.

    this also returns a list of shard ids, which can
    just be all of them.  but here we'll search into the Query in order
    to try to narrow down the list of shards to query.

    """
    ids = []

    for column, operator, value in _get_query_comparisons(query):
        # "shares_lineage()" returns True if both columns refer to the same
        # statement column, adjusting for any annotations present.
        if column.shares_lineage(Staff.__table__.c.store) \
                or column.shares_lineage(Customer.__table__.c.store) \
                or column.shares_lineage(Product.__table__.c.store) \
                or column.shares_lineage(Stock.__table__.c.store) \
                or column.shares_lineage(Order.__table__.c.store) \
                or column.shares_lineage(OrderItem.__table__.c.store):
            if operator == operators.eq:
                ids.append(shard_lookup[value])
            elif operator == operators.in_op:
                ids.extend(shard_lookup[v] for v in value)

    if len(ids) == 0:
        return ["postgres", "sql_server", "sqlite"]
    else:
        return ids


def _get_query_comparisons(query):
    """Search an orm.Query object for binary expressions.

    Returns expressions which match a Column against one or more
    literal values as a list of tuples of the form
    (column, operator, values).   "values" is a single value
    or tuple of values depending on the operator.

    """
    binds = {}
    clauses = set()
    comparisons = []

    def visit_bindparam(bind):
        # visit a bind parameter.

        # check in _params for it first
        if bind.key in query._params:
            value = query._params[bind.key]
        elif bind.callable:
            # some ORM functions (lazy loading)
            # place the bind's value as a
            # callable for deferred evaluation.
            value = bind.callable()
        else:
            # just use .value
            value = bind.value

        binds[bind] = value

    def visit_column(column):
        clauses.add(column)

    def visit_binary(binary):
        # special handling for "col IN (params)"
        if (
            binary.left in clauses
            and binary.operator == operators.in_op
            and hasattr(binary.right, "clauses")
        ):
            comparisons.append(
                (
                    binary.left,
                    binary.operator,
                    tuple(binds[bind] for bind in binary.right.clauses),
                )
            )
        elif binary.left in clauses and binary.right in binds:
            comparisons.append(
                (binary.left, binary.operator, binds[binary.right])
            )

        elif binary.left in binds and binary.right in clauses:
            comparisons.append(
                (binary.right, binary.operator, binds[binary.left])
            )

    # here we will traverse through the query's criterion, searching
    # for SQL constructs.  We will place simple column comparisons
    # into a list.
    if query._criterion is not None:
        visitors.traverse_depthfirst(
            query._criterion,
            {},
            {
                "bindparam": visit_bindparam,
                "binary": visit_binary,
                "column": visit_column,
            },
        )
    return comparisons


# further configure create_session to use these functions
create_session.configure(
    shard_chooser=shard_chooser,
    id_chooser=id_chooser,
    query_chooser=query_chooser,
)

if __name__ == "__main__":
    for db in (sql_server, postgres, sqlite):
        Base.metadata.drop_all(db)
        Base.metadata.create_all(db)

    sess = create_session()
    # save and load objects!
    staff_list = list()
    for i in range(0, 100):
        profile = fake.simple_profile()
        staff_list.append(Staff(profile["name"].split(" ")[0], profile["name"].split(" ")[
            1], profile["mail"], random.choice([True, False]), random.choice(["Kenya", "Uganda", "Tanzania"])))
    sess.add_all(staff_list)

    # TODO: Add orders 200
    customers_list = list()
    for i in range(0, 40):
        profile = fake.simple_profile()
        customers_list.append(Customer(profile["name"].split(" ")[0], profile["name"].split(" ")[
            1], profile["mail"], fake.city(), random.choice(["Kenya", "Uganda", "Tanzania"])))
    sess.add_all(customers_list)

    products_list = list()
    for i in range(0, 30):
        products_list.append(Product(fake.color_name().capitalize()+" "+random.choice(
            ["T-shirt", "Jeans", "Vest", "Knicker", "Underpants"]), random.randint(250, 5000), random.choice(["Kenya", "Uganda", "Tanzania"])))
    sess.add_all(products_list)

    stock_list = list()
    for product in products_list:
        stock_list.append(Stock(product, random.randint(5, 100)))
    sess.add_all(stock_list)

    orders_list = list()
    for customer in customers_list:
        store = customer.store
        el_staff = [staff for staff in filter(
            lambda staff: staff.store == store, staff_list)]
        el_products = [product for product in filter(
            lambda product: product.store == store, products_list)]
        order = Order(customer, random.choice(staff_list), store,
                      True, fake.date_time_between('-2y'))
        order.order_items.append(
            OrderItem(order, random.choice(el_products), random.randint(1, 4)))
        orders_list.append(order)
    sess.add_all(orders_list)

    sess.commit()

    t = sess.query(Staff).get(staff_list[0].id)
    print(t.first_name, t.email, t.store)

    kenyan_staff = sess.query(Staff).filter(
        Staff.store == "Kenya"
    )
    print(len(kenyan_staff.all()))

    ug_and_tz_staff = sess.query(Staff).filter(
        Staff.store.in_(["Uganda", "Tanzania"])
    )
    print(len(ug_and_tz_staff.all()))
