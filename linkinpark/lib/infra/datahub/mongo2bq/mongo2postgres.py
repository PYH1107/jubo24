import collections
import csv
import json
import os
from datetime import datetime, timedelta
from io import StringIO

import numpy as np
import pandas as pd
from bson import json_util
from bson.code import Code
from tqdm import tqdm

from linkinpark.lib.common.logger import CloudLogger, getLogger
from linkinpark.lib.common.mongo_connector import MongodbNISReadOnly
from linkinpark.lib.common.postgres_connector import PostgresConnectorFactory

DATASET_POSTFIX = os.environ.get("DATASET_POSTFIX", "test")
LOGGER_LABELS = {
    'datarch': 'infra_datahub_mongo2bq',
    'team': 'infra',
    'env': DATASET_POSTFIX,
    'app': 'datahub',
    'process': 'mongo2postgres',
}

logger = getLogger(name='Mongo2Postgres', labels=LOGGER_LABELS)


class Mongo2Postgres:
    def __init__(self):
        self.nis_db = MongodbNISReadOnly(
            selection_timeout=2000000)
        self.postgres_db = PostgresConnectorFactory.get_cloudsql_postgres_connector(
            dbname="mongo", mode=DATASET_POSTFIX)

    def drop_and_create_table(self, collection_name, columns, columns_dtypes):

        self.postgres_db.execute_sql_command(
            f'DROP TABLE IF EXISTS "{collection_name}"')
        self.postgres_db.create_table(collection_name, columns, columns_dtypes)

    def get_collection_columns(self, collection_name):
        map = Code("""function() {for (var key in this){ emit(key, null);}}""")
        reduce = Code("""function(key, value){ return null;}""")
        result_dict = dict()
        result = self.nis_db[collection_name].inline_map_reduce(
            map, reduce, result_dict)

        # The "if" condition was for multi layers of dict's key
        # example: {a: {b: c}} so the "col" is "a.b" but we expect get "a" enough.
        columns = [r["_id"] for r in result if '.' not in r["_id"]]

        # get first value to find dtype
        columns_dtypes = []
        for col in columns:
            record = self.nis_db[collection_name].find_one(
                {f"{col}": {'$exists': True}})
            json_doc = json.loads(json_util.dumps(record))
            json_doc = trans_column_value(json_doc)
            val = json_doc[col]
            columns_dtypes.append(type(val))

        return columns, columns_dtypes

    def format_json_cell(self, data, total_columns, columns_dtypes):
        data = data.copy()
        json_keys = []

        for c, d in zip(total_columns, columns_dtypes):
            if d == dict or d == list:
                json_keys.append(c)

        for k, v in data.items():
            if k in json_keys:
                if isinstance(v, dict) or isinstance(v, list):
                    data[k] = json.dumps(v, ensure_ascii=False)
                elif isinstance(v, str):
                    data[k] = json.dumps({v: None}, ensure_ascii=False)
        return data

    def submit_doc_list_uploading(self, collection_name, doc_list, total_columns, json_keys, is_finish=False):
        df = pd.DataFrame.from_records(doc_list)
        df = df.reindex(columns=total_columns)
        # make sure order
        df = df.loc[:, total_columns]

        # replace json columns nan value to string by json dumps
        for col in json_keys:
            df[col] = df[col].replace(np.nan, json.dumps([]))
            df[col] = df[col].replace('', json.dumps([]))

        # upload dataframe with IO streaming
        table_name = collection_name

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

        self.postgres_db._cur.copy_expert(
            f'''COPY "{table_name}" FROM STDIN WITH ''' + """(FORMAT CSV,DELIMITER E'\t',QUOTE '\r', ESCAPE '\\')""", output)
        self.postgres_db.commit()

        # self.multi_postgres_worker.put_job(
        #     {"df": df, "table_name": collection_name, "is_finish": is_finish})

    def sync_mongo2postgres(self, collection_name, reset_column=False):
        temp_collection_name = f'__{collection_name}'
        total_row = self.nis_db[collection_name].estimated_document_count(
        )
        logger.debug(
            f"collection: {collection_name}, number of rows: {total_row}")

        if reset_column:
            # get all keys from collections
            total_columns, columns_dtypes = self.get_collection_columns(
                collection_name)

            # remove duplicate collections (none case sensitive)
            lower_total_columns = [c.lower() for c in total_columns]
            duplicated_columns = [item for item, count in collections.Counter(
                lower_total_columns).items() if count > 1]
            for d in duplicated_columns:
                logger.debug(f"Error! find duplicate column: {d}")
                rm_idx = total_columns.index(d)
                total_columns.pop(rm_idx)
                columns_dtypes.pop(rm_idx)
        else:
            # get current columns
            total_columns, columns_dtypes = self.postgres_db.get_columns(
                collection_name)

        # get json dtype columns
        json_keys = []
        for c, d in zip(total_columns, columns_dtypes):
            if d == dict or d == list:
                json_keys.append(c)

        # drop and create temp table
        self.drop_and_create_table(
            temp_collection_name, total_columns, columns_dtypes)

        doc_list = []
        count = 0
        for cursor in tqdm(self.nis_db[collection_name].find({}).sort([('$natural', 1)])):
            count += 1
            json_doc = json.loads(json_util.dumps(cursor))
            json_doc = trans_column_value(json_doc)
            json_doc = self.format_json_cell(
                json_doc, total_columns, columns_dtypes)

            doc_list.append(json_doc)

            # give limitation for unit tests
            if DATASET_POSTFIX == "test" and count == 999:
                break

            if len(doc_list) > 5000:
                self.submit_doc_list_uploading(
                    temp_collection_name, doc_list, total_columns, json_keys)
                doc_list = []

        self.submit_doc_list_uploading(
            temp_collection_name, doc_list, total_columns, json_keys, is_finish=True)

        # waiting for all jobs complete
        # self.multi_postgres_worker.join()

        # drop collection_name table if exist
        sql = f'DROP TABLE IF EXISTS "{collection_name}"'
        self.postgres_db.execute_sql_command(sql)

        # rename '__<collection_name> to <collection_name>
        sql = f'ALTER TABLE "{temp_collection_name}" RENAME TO "{collection_name}";'
        self.postgres_db.execute_sql_command(sql)
        self.postgres_db.commit()


# -------------
# Transform functions below


def trans_column_value(col):
    if isinstance(col, dict):
        if _is_mongo_dtype(col):
            col = _trans_mongo_dtype(col)
        else:
            for k, v in col.items():
                col[k] = trans_column_value(v)
    elif isinstance(col, list):
        for index, item in enumerate(col):
            if _is_mongo_dtype(item):
                col[index] = _trans_mongo_dtype(item)
            else:
                col[index] = trans_column_value(item)
    elif isinstance(col, (str, int, float, type(None))):
        col = str(col)
        # replace some char made uploading to postgres error
        if '"' in col:
            col = col.replace('"', '')
    else:
        raise TypeError(F"Type {type(col)}, is unable to transform.")

    return col


def _trans_mongo_dtype(d: dict):
    k, v = list(d.items())[0]
    if k == "$oid":
        k = v
    elif k == "$date":
        if isinstance(v, dict):
            k = (datetime(1970, 1, 1) +
                 timedelta(seconds=(int(v["$numberLong"]) / 1000)))
        elif isinstance(v, int):
            k = (datetime(1970, 1, 1) +
                 timedelta(seconds=(int(v) / 1000)))
        else:
            k = pd.to_datetime(v)
        k = k.strftime("%Y-%m-%d %H:%M:%S.%f")
    else:
        raise TypeError("Unrecognized MongoDB data type.")
    return k


def _is_mongo_dtype(d: dict):
    mongo_dtype = False
    if isinstance(d, dict):
        if len(d) == 1:
            k = list(d.keys())[0]
            if k.startswith("$"):
                mongo_dtype = True
    return mongo_dtype
