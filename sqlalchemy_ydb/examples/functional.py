from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import logging
import time

from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.dialects import registry
from sqlalchemy.sql import select, expression, operators
from sqlalchemy.orm import sessionmaker

from sqlalchemy_ydb import YdbDialect


def simple_select(session):
    two = expression.literal(2)
    statement1 = select([two.label('i')])
    statement2 = select([operators.mul(two, two).label('i')])
    union = statement1.union_all(statement2)

    for row in session.execute(union):
        print(row.i)


def simple_users_manipulation(session):
    metadata = MetaData()
    users = Table('usersss',
          metadata,
          Column('id', Integer, primary_key=True, nullable=True),
          Column('name', String, nullable=True),
          Column('fullname', String, nullable=True)
    )

    metadata.create_all(session.get_bind())

    ins = users.insert().values(id=int(time.time()), name='jack', fullname='Jack Jones')
    session.execute(ins)

    session.execute(users.select())
    for row in session.execute(users.count()):
        print(row[0])



def run(args):
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.ERROR)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    registry.impls['ydb'] = YdbDialect

    engine = create_engine(args.connection_string)
    Session = sessionmaker(bind=engine, autoflush=True)
    session = Session()

    simple_select(session)

    simple_users_manipulation(session)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--connection-string', '-c', dest='connection_string', type=str, required=True)
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true')
    run(parser.parse_args())
