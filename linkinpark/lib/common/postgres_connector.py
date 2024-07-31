from __future__ import annotations

import csv
import json
import multiprocessing as mp
import queue
from datetime import datetime
from io import StringIO
from typing import Any

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import extras

DTYPE_MAPPING_TABLE = {
    str: "text",
    int: "integer",
    bool: "text",
    list: "json",
    dict: "json",
    float: "double precision",
    datetime: "timestamp"
}

INVERSE_DTYPE_MAPPING_TABLE = {
    "text": str,
    "integer": int,
    "double precision": float,
    "json": dict,
    "timestamp": datetime,
}


class PostgresConnectorFactory:
    """A virtual object that used to get predefined postgres connector."""

    def __init__(self) -> None:
        raise NotImplementedError

    @staticmethod
    def get_cloudsql_postgres_connector(dbname, mode=None, schema='public'):
        from .secret_accessor import SecretAccessor

        if not dbname:
            raise ValueError('dbname is required')

        sa = SecretAccessor()

        target_dbname = f'{dbname}_{mode}' if mode else dbname

        params = {
            'dbname': target_dbname, 
            'options': f'-c search_path={schema}'
        }

        secret_name_template = 'datahub-postgres-default-{}'
        for param in ['host', 'user', 'password']:
            params[param] = sa.access_secret(
                secret_name_template.format(param))
            
        conn = PostgresConnector(**params)

        if schema != dict(conn.fetch_all('SELECT CURRENT_SCHEMA;')[0])['current_schema']:
            raise ValueError('schema not valid')

        return conn


class PostgresConnector:
    def __init__(self, *args, **kwargs):
        """To construct a connector for accessing postgres.
        Must provide a single connection string or keyword arguments or a mix of both,
        which kwargs will have precedence over dsn value.\n
        For original usage, please refer to PostgresConnectorFactory.get_cloudsql_postgres_connector().
        It's recommended using `with` to manage context.
        e.g. with PostgresConnector(**params) as connector: ...

        Arguments:
            dsn -- a libpq connection string.

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
        self._conn = psycopg2.connect(*self._args, **self._kwargs)
        self._conn.autocommit = True
        self._cur = self._conn.cursor(cursor_factory=extras.RealDictCursor)

    def _close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, trackback):
        self._close()

    def run_sql_bulk_execute(self, query, values):
        """
        Bulk Insert Example Usage:
            query = 'insert into json_tbl("id","mem","member") values %s'
            values = []
            for v in range(2):
                d = {"hi": "hihi"}
                values.append((v, "yoyo", json.dumps(d)))
            connector.run_sql_bulk_execute(query, values)
        """
        print(f"Run bulk insert: {query}, {values}")
        extras.execute_values(self._cur, query, values,
                              page_size=values.__len__())

    def run_sql_execute(self, query, values):
        """
        Example Usage:
            query = 'insert into json_tbl("id","mem","member") values %s'
            values = ("0", "yoyo", json.dumps(d))
            connector.run_sql_execute(query, values)
        """
        print(f"Run insert: {query}, {values}")
        self._cur.execute(query, values)

    def execute_sql_command(self, sql):
        """
        Directly execute sql command
        """
        print(f'Run sql: {sql}')
        self._cur.execute(sql)

    def fetch_all(self, query=None):

        if query:
            self._cur.execute(query)

        return self._cur.fetchall()

    def create_table(self, table_name, column_names, dtypes, constraints: dict = []):
        columns_str = ', '.join(
            [f'"{col}" {DTYPE_MAPPING_TABLE[dtype] if isinstance(dtype, type) else dtype}'
                for col, dtype in zip(column_names, dtypes)])

        constraints_str = \
            (', ' + ', '.join([f'CONSTRAINT "{k}" {v}'
                               for k, v in constraints.items()])) if constraints else ''

        sql = 'Create table IF NOT EXISTS "%s" ( %s%s )' % (
            table_name, columns_str, constraints_str)

        print('Create table: ', sql)
        self.execute_sql_command(sql)

    def commit(self):
        self._conn.commit()

    def insert_values(self, table_name: str, columns: list[str], values: list[tuple[Any]], returning: list[str] = None, page_size=1000):
        cmd_insert = """
        INSERT INTO "{}"({}) VALUES {}{};
        """
        columns_quote = ', '.join([f'"{x}"' for x in columns])

        if returning:
            returning_quote = f" RETURNING {', '.join(returning)}"
            rows = [f"({', '.join([self._values_quoting(col) for col in row])})"
                    for row in values]

            self._cur.execute(cmd_insert.format(
                table_name, columns_quote, ',\n'.join(rows), returning_quote))

            if len(returning) == 1:
                return [row[returning[0]] for row in self._cur.fetchall()]
            else:
                return [tuple(map(row.get, returning)) for row in self._cur.fetchall()]

        else:
            pages, res = divmod(len(values), page_size)
            pages = pages + 1 if res else pages

            for page in range(pages):
                offset = page * page_size
                rows = [f"({', '.join([self._values_quoting(col) for col in row])})"
                        for row in values[offset:offset + page_size]]
                self._cur.execute(cmd_insert.format(
                    table_name, columns_quote, ',\n'.join(rows), ''))

    def _values_quoting(self, value, quote="$quo$"):
        import re
        import unicodedata

        if isinstance(value, list):
            return f"ARRAY[{', '.join([self._values_quoting(x) for x in value])}]"
        if isinstance(value, datetime):
            return f"TIMESTAMP '{str(value)}'"
        if pd.isna(value) | (value == ''):
            return "NULL"

        str_value = unicodedata.normalize('NFKC', str(value))
        if re.match(r'^\d+(?:\.\d+)?$', str_value, flags=re.ASCII):
            return str_value
        else:
            return f"{quote}{str_value}{quote}"

    def select_values(self, table_name: str, columns: list[str] = None, filter: list[str] = None, groupby: list[str] = None, range: tuple[int, int] = None):
        cmd_select = """
        SELECT {} FROM "{}"{}{}{};
        """
        select_quote = ', '.join(columns) if columns else "*"
        filter_quote = f' WHERE {" AND ".join(filter)}' if filter else ''
        groupby_quote = f' GROUP BY {", ".join(groupby)}' if groupby else ''
        range_quote = f' ORDER BY 1 LIMIT {range[1]} OFFSET {range[0]}' if range else ''
        self._cur.execute(cmd_select.format(
            select_quote, table_name, filter_quote, groupby_quote, range_quote))
        return self._cur.fetchall()

    def update_values(self, table_name: str, columns: list[str], values: list[str], filter: list[str]):
        cmd_update = """
        UPDATE "{}" SET {} WHERE {};
        """
        update_quote = ', '.join(
            [f'{c}={self._values_quoting(v)}' for c, v in zip(columns, values)])
        filter_quote = " AND ".join(filter)
        self._cur.execute(cmd_update.format(
            table_name, update_quote, filter_quote))

    def delete_values(self, table_name: str, filter: list[str]):
        cmd_delete = """
        DELETE FROM "{}" WHERE {};
        """
        filter_quote = " AND ".join(filter)
        self._cur.execute(cmd_delete.format(
            table_name, filter_quote))

    def get_columns(self, table_name):
        """
        Args:
            table_name: string of table name
        Output:
            column_names <list>: list of column name
            column_dtype <list>: list of dtypes
        """
        query = """
            SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema='public' 
                AND table_name='{}'
        """
        self._cur.execute(query.format(table_name))
        res = self.fetch_all(query.format(table_name))

        column_names = []
        column_dtypes = []
        for row in res:
            row = json.loads(json.dumps(row))
            column_name = row['column_name']
            dtype = row['data_type']
            dtype = INVERSE_DTYPE_MAPPING_TABLE[dtype]

            column_names.append(column_name)
            column_dtypes.append(dtype)
        return column_names, column_dtypes

    @property
    def undefined_table_errors(self):
        return psycopg2.errors.UndefinedTable

    @property
    def undefined_column_errors(self):
        return psycopg2.errors.UndefinedColumn


class MultiplePostgresWorker:
    """
    MultiplePostgresWorker is used to speed up postgres operations
    You can customize "job_func" to implement your function
    and "put_job" into job queue.
    """

    def __init__(self, num_worker=3, dbname='mongo', mode='test'):
        self.dbname = dbname
        self.mode = mode

        self.q = mp.Queue(maxsize=num_worker * 10)
        self.terminate_queue = mp.Queue(maxsize=num_worker * 10)
        self.is_terminal = False

        self.workers = []
        for _ in range(num_worker):
            t = mp.Process(target=self._job, args=(
                self.q, self.terminate_queue))
            t.daemon = True
            t.start()
            self.workers.append(t)

    def job_func(self, job, connector):
        print("This function can be overwrited!")
        print("get job:", job)
        is_finish = job.get('is_finish', False)
        sql = 'SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema=\'public\' ORDER BY table_schema,table_name'
        connector.execute_sql_command(sql)
        res = connector.fetch_all()
        print(f"result:{res}")

        if is_finish:
            self.finish()

    def put_job(self, job):
        self.q.put(job)

    def finish(self):
        print("MultiplePostgresWorker finish")
        self.terminate_queue.put(1)

    def _job(self, q, terminate_queue):
        connector = PostgresConnectorFactory.get_cloudsql_postgres_connector(
            dbname=self.dbname, mode=self.mode)
        while True:
            try:
                job = q.get(timeout=300)
                self.job_func(job, connector)

            except queue.Empty:
                self.finish()
                break
            except Exception as e:
                self.finish()
                print(f"failed:{e}")
                break
        print("done")

    def join(self):
        if self.terminate_queue.get():
            print(self.workers)
            for w in self.workers:
                w.terminate()
        if self.q.qsize() > 0:
            raise BufferError(
                f"failed: Queue size is not empty:{self.q.qsize()}")
        self.q.close()
        self.terminate_queue.close()


class DataframeMultipleUploader(MultiplePostgresWorker):
    """
    DataframeMultipleUploader is used to upload dataframe with IO streaming
    """

    def job_func(self, job, connector, is_finish=False):
        print("get job:", job)
        df = job['df']
        table_name = job['table_name']
        is_finish = job.get('is_finish', False)
        # replace nan and '' to NULL to avoid to_csv failed
        df = df.replace(np.nan, 'NULL')
        df = df.replace('', 'NULL')

        df.replace(to_replace=[r"\x00|\\t|\\n|\\r|\\b|\\", "|\t|\n|\r|\b|"], value=[
            "", ""], regex=True, inplace=True)

        output = StringIO()
        df.to_csv(output, sep='\t', index=False,
                  header=False, quoting=csv.QUOTE_NONE, escapechar='\r')
        output.getvalue()
        output.seek(0)

        connector._cur.copy_expert(
            f'''COPY "{table_name}" FROM STDIN WITH ''' + """(FORMAT CSV,DELIMITER E'\t',QUOTE '\r', ESCAPE '\\')""", output)
        connector.commit()

        if is_finish:
            self.finish()
