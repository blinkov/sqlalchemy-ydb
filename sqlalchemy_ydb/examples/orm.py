from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import logging

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.dialects import registry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_ydb import YdbDialect

Base = declarative_base()


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, nullable=True)
    name = Column(String, nullable=True)
    fullname = Column(String, nullable=True)

    def __init__(self, id, name, fullname):
        self.id = id
        self.name = name
        self.fullname = fullname

    def __repr__(self):
        return 'User: %s' % (self.fullname or self.name or '<no name>')


def orm_example(session):
    User.metadata.create_all(session.get_bind())

    session.bulk_save_objects([
        User(1, 'John', 'John Smith'),
        User(2, 'Joe', 'Joe Smith'),
        User(3, 'John', 'John Doe')
    ])
    session.commit()

    query = session.query(User.fullname) \
        .filter(User.name == 'John')

    for fullname, in query:
        print(fullname)

    print('Count: ', query.count())


def run(args):
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.ERROR)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    registry.impls['ydb'] = YdbDialect

    engine = create_engine(args.connection_string)
    Session = sessionmaker(bind=engine, autoflush=True)
    session = Session()

    orm_example(session)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--connection-string', '-c', dest='connection_string', type=str, required=True)
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true')
    run(parser.parse_args())
