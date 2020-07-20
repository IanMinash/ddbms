import datetime
import os
import uuid
import random
import mysql.connector


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
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import operators
from sqlalchemy.sql import visitors
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
    f"postgresql://chairman:salatonElvis@127.0.0.1:5432/ddbms")
sql_server = create_engine(
    f"mysql+pymysql://root:$krychowiak-254$@localhost/ddbms")
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


# create tables
for db in (sql_server, postgres, sqlite):
    Base.metadata.drop_all(db)
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
        if column.shares_lineage(Staff.__table__.c.store):
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

# save and load objects!
staff = list()
for i in range(0, 100):
    profile = fake.simple_profile()
    staff.append(Staff(profile["name"].split(" ")[0], profile["name"].split(" ")[
                 1], profile["mail"], random.choice([True, False]), random.choice(["Kenya", "Uganda", "Tanzania"])))


sess = create_session()

sess.add_all(staff)

sess.commit()

t = sess.query(Staff).get(staff[0].id)
print(t.first_name, t.email, t.store)


kenyan_staff = sess.query(Staff).filter(
    Staff.store == "Kenya"
)
print(len(kenyan_staff.all()))

ug_and_tz_staff = sess.query(Staff).filter(
    Staff.store.in_(["Uganda", "Tanzania"])
)
print(len(ug_and_tz_staff.all()))
