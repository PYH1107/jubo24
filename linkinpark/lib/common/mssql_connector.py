from __future__ import annotations

import pymssql


class MssqlConnectorFactory:
    """A virtual object that used to get predefined mssql connector."""

    def __init__(self) -> None:
        raise NotImplementedError

    @staticmethod
    def get_caring_connector(host, dbname):
        from .secret_accessor import SecretAccessor
        sa = SecretAccessor()

        params = {'host': host, 'dbname': dbname}

        secret_name_template = 'datahub-caring-mssql-{}'
        for param in ['port', 'user', 'password']:
            params[param] = sa.access_secret(
                secret_name_template.format(param))

        return MssqlConnector(**params)


class MssqlConnector:
    def __init__(self, *args, **kwargs):
        """To construct a connector for accessing MSSQL.
        Must provide keyword arguments shown below.\n
        It's recommended using `with` to manage context.\n
        e.g. with MssqlConnector(**params) as connector: ...  

        Keyword Arguments:
            host -- IP address or domain name.\n
            port -- post number.\n
            user -- user name authentication.\n
            password -- password of authentication.\n
            dbname -- database name.\n

        Returns:
            The connector.
        """
        self._args = args
        self._kwargs = kwargs
        self._open()

    def _open(self):
        dbname = self._kwargs.pop('dbname')
        self._conn = pymssql.connect(
            database=dbname, *self._args, **self._kwargs, autocommit=True)
        self._cur = self._conn.cursor(as_dict=True)

    def _close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, trackback):
        self._close()

    def select_values(self, table_name: str, columns: list[str] = None, filter: str = None, groupby: list[str] = None, range: tuple[int, int] = None):
        cmd_select = """
        SELECT {} FROM [{}]{}{}{};
        """
        select_quote = ', '.join(columns) if columns else "*"
        filter_quote = f' WHERE {filter}' if filter else ''
        groupby_quote = f' GROUP BY {", ".join(groupby)}' if groupby else ''
        range_quote = f' ORDER BY 1 OFFSET {range[0]} ROWS FETCH NEXT {range[1]} ROWS ONLY' if range else ''
        self._cur.execute(cmd_select.format(
            select_quote, table_name, filter_quote, groupby_quote, range_quote))
        return self._cur.fetchall()

    def fetch_all(self, query: str):
        self._cur.execute(query)
        return self._cur.fetchall()
