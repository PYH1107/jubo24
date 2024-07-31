
import os
import json

import csv
import collections
import numpy as np
import pandas as pd
from tqdm import tqdm
from io import StringIO
from bson.code import Code
from bson import json_util

from linkinpark.lib.common.mongo_connector import MongodbNISReadOnly
from linkinpark.lib.infra.datahub.mongo2postgres.trans_worker import trans_column_value
from linkinpark.lib.common.postgres_connector import PostgresConnector, PostgresConnectorFactory

import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

JUBO_WORKSTATE = os.environ.get("JUBO_WORKSTATE", "test")
MONGO_DB_NAME = 'release'


class DBSyncer:
    def __init__(self):
        self.nis_db = MongodbNISReadOnly()

        self.connector = PostgresConnectorFactory.get_cloudsql_postgres_connector(
            dbname="mongo", mode=JUBO_WORKSTATE)

    def upload_df(self, collection_name, df, total_columns, json_keys):
        df = df.reindex(columns=total_columns)
        # make sure order
        df = df.loc[:, total_columns]

        # replace json columns nan value to string by json dumps
        for col in json_keys:
            df[col] = df[col].replace(np.nan, json.dumps([]))
            df[col] = df[col].replace('', json.dumps([]))

        # replace nan and '' to NULL to avoid to_csv failed
        df = df.replace(np.nan, 'NULL')
        df = df.replace('', 'NULL')

        df.replace(to_replace=[r"\x00|\\t|\\n|\\r|\\b|\\", "|\t|\n|\r|\b|"], value=[
            "", ""], regex=True, inplace=True)

        if len(df) > 5000:
            partitions = int(len(df) / 5000)
            dfs = np.array_split(df, partitions)
        else:
            dfs = [df]

        for _df in tqdm(dfs):
            # simple but slowly method
            # _df.to_sql(collection_name, self.conn, index=False,
            #            method='multi', if_exists='append')

            # used to debug
            # _df.to_csv("temp.csv", sep='\t', index=False,
            #            header=True, quoting=csv.QUOTE_NONE)

            output = StringIO()
            _df.to_csv(output, sep='\t', index=False,
                       header=False, quoting=csv.QUOTE_NONE, escapechar='\r')

            output.getvalue()
            output.seek(0)

            # deprecate: copy_from will replace double quote to empty into json columns
            # self.connector.cur.copy_from(output, collection_name)

            self.connector._cur.copy_expert(
                f'''COPY "{collection_name}" FROM STDIN WITH ''' + """(FORMAT CSV,DELIMITER E'\t',QUOTE '\r', ESCAPE '\\')""", output)

            self.connector.commit()

    def drop_and_create_table(self, collection_name, columns, columns_dtypes):

        self.connector.execute_sql_command(
            f'DROP TABLE IF EXISTS "{collection_name}"')
        self.connector.create_table(collection_name, columns, columns_dtypes)

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

    def sync_mongo2postgres(self, collection_name):
        temp_collection_name = f'__{collection_name}'
        total_row = self.nis_db[collection_name].estimated_document_count(
        )
        print(
            f"collection: {collection_name}, number of rows: {total_row}")

        # get all keys from collections
        total_columns, columns_dtypes = self.get_collection_columns(
            collection_name)

        # remove duplicate collections (none case sensitive)
        lower_total_columns = [c.lower() for c in total_columns]
        duplicated_columns = [item for item, count in collections.Counter(
            lower_total_columns).items() if count > 1]
        for d in duplicated_columns:
            print(f"Error! find duplicate column: {d}")
            rm_idx = total_columns.index(d)
            total_columns.pop(rm_idx)
            columns_dtypes.pop(rm_idx)

        # get json dtype columns
        json_keys = []
        for c, d in zip(total_columns, columns_dtypes):
            if d == dict or d == list:
                json_keys.append(c)

        # drop and create table
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
            if JUBO_WORKSTATE == "test" and count == 999:
                break

            if len(doc_list) > 100000:
                df = pd.DataFrame.from_records(doc_list)
                self.upload_df(temp_collection_name, df,
                               total_columns, json_keys)
                doc_list = []

        df = pd.DataFrame.from_records(doc_list)
        self.upload_df(temp_collection_name, df, total_columns, json_keys)

        # drop collection_name table if exist
        sql = f'DROP TABLE IF EXISTS "{collection_name}"'
        self.connector.execute_sql_command(sql)

        # rename '__<collection_name> to <collection_name>
        sql = f'ALTER TABLE "{temp_collection_name}" RENAME TO "{collection_name}";'
        self.connector.execute_sql_command(sql)
        self.connector.commit()
