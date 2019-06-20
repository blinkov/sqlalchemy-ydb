from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import argparse
import logging

from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.sql import select, expression, operators

from sqlalchemy_ydb import YdbDialect


def run(args):
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.ERROR)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    registry.impls['ydb'] = YdbDialect

    engine = create_engine(args.connection_string, echo=True)

    two = expression.literal(2)
    statement1 = select([two.label('i')])
    statement2 = select([operators.mul(two, two).label('i')])
    union = statement1.union_all(statement2)

    for row in engine.execute(union):
        print(row.i)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--connection-string', '-c', dest='connection_string', type=str, required=True)
    parser.add_argument('--verbose', '-v', dest='verbose', action='store_true')
    run(parser.parse_args())
