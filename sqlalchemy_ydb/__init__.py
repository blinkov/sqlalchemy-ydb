from __future__ import absolute_import
from __future__ import unicode_literals

from kikimr.public.dbapi.errors import NotSupportedError


try:
    from sqlalchemy.engine.default import DefaultDialect, DefaultExecutionContext
    from sqlalchemy.sql.compiler import DDLCompiler, IdentifierPreparer, GenericTypeCompiler

    class YdbIdentifierPreparer(IdentifierPreparer):
        def __init__(self, dialect):
            super(YdbIdentifierPreparer, self).__init__(dialect,
                                                        initial_quote='`',
                                                        final_quote='`')

    class YdbDDLCompiler(DDLCompiler):
        raw_create_table = None

        def visit_create_table(self, create):
            self.raw_create_table = create
            return super(YdbDDLCompiler, self).visit_create_table(create)


    class YdbTypeCompiler(GenericTypeCompiler):
        def _render_string_type(self, type_, name):
            text = name
            if type_.length:
                pass
            if type_.collation:
                raise NotSupportedError
            return text


    class YdbExecutionContext(DefaultExecutionContext):
        pass


    class YdbDialect(DefaultDialect):
        name = 'ydb'
        supports_alter = False
        max_identifier_length = 255
        supports_sane_rowcount = False

        supports_native_enum = False
        supports_native_boolean = True
        supports_smallserial = False

        supports_sequences = False
        sequences_optional = True
        preexecute_autoincrement_sequences = True
        postfetch_lastrowid = False

        supports_default_values = False
        supports_empty_insert = False
        supports_multivalues_insert = True
        default_paramstyle = 'qmark'

        isolation_level = None

        preparer = YdbIdentifierPreparer
        ddl_compiler = YdbDDLCompiler
        type_compiler = YdbTypeCompiler
        execution_ctx_cls = YdbExecutionContext

        @staticmethod
        def dbapi():
            import kikimr.public.dbapi
            return kikimr.public.dbapi

        def has_table(self, connection, table_name, schema=None):
            if schema is not None:
                raise NotSupportedError

            return False  # TODO

        def _check_unicode_returns(self, connection, additional_tests=None):
            return True

except ImportError:
    class YdbDialect(object):
        def __init__(self):
            raise RuntimeError('could not import sqlalchemy')
