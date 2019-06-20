from __future__ import absolute_import
from __future__ import unicode_literals

from kikimr.public.dbapi.errors import NotSupportedError


try:
    from sqlalchemy.engine.default import DefaultDialect
    from sqlalchemy.sql.compiler import IdentifierPreparer, GenericTypeCompiler

    class YdbIdentifierPreparer(IdentifierPreparer):
        def __init__(self, dialect):
            super(YdbIdentifierPreparer, self).__init__(dialect,
                                                        initial_quote='`',
                                                        final_quote='`')

    class YdbTypeCompiler(GenericTypeCompiler):
        def _render_string_type(self, type_, name):
            text = name
            if type_.length:
                pass
            if type_.collation:
                raise NotSupportedError
            return text


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
        type_compiler = YdbTypeCompiler

        def __init(self, **kwargs):
            super(DefaultDialect, self).__init__(**kwargs)

        @staticmethod
        def dbapi():
            import kikimr.public.dbapi
            return kikimr.public.dbapi

        def has_table(self, connection, table_name, schema=None):
            if schema is not None:
                raise NotSupportedError

            return False  # TODO

        def _execute_ddl(self, ddl, multiparams, params):
            """Execute a schema.DDL object."""
            print(locals())
            exit(123)
            if self._has_events or self.engine._has_events:
                for fn in self.dispatch.before_execute:
                    ddl, multiparams, params = fn(self, ddl, multiparams, params)

            dialect = self.dialect

            compiled = ddl.compile(
                dialect=dialect,
                schema_translate_map=self.schema_for_object
                if not self.schema_for_object.is_default
                else None,
            )
            ret = self._execute_context(
                dialect,
                dialect.execution_ctx_cls._init_ddl,
                compiled,
                None,
                compiled,
            )
            if self._has_events or self.engine._has_events:
                self.dispatch.after_execute(self, ddl, multiparams, params, ret)
            return ret



except ImportError:
    class YdbDialect(object):
        def __init__(self):
            raise RuntimeError('could not import sqlalchemy')
