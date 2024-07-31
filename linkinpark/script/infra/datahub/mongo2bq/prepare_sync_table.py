import os
import sys
from datetime import datetime

from linkinpark.lib.common.logger import Logger
from linkinpark.lib.common.mongo_connector import MongodbNISReadOnly
from linkinpark.lib.common.postgres_connector import PostgresConnector, PostgresConnectorFactory

NUM_WORKERS = int(sys.argv[1])

SYNC_LOCKER_DB_NAME = "mongo2postgres"
DATASET_POSTFIX = os.environ.get("DATASET_POSTFIX", "test")
SYNC_LOCKER_TABLE_NAME = f"sync_table_{DATASET_POSTFIX}"
LOGGER_LABELS = {
    'datarch': 'infra_datahub_mongo2bq',
    'team': 'infra',
    'env': DATASET_POSTFIX,
    'app': 'datahub',
    'process': 'prepare_sync_table',
}

logger = Logger(labels=LOGGER_LABELS)

conn = PostgresConnectorFactory.get_cloudsql_postgres_connector(
    dbname=SYNC_LOCKER_DB_NAME, mode=None)

sql = f"Create table IF NOT EXISTS {SYNC_LOCKER_TABLE_NAME}"\
    " (collection_name text, last_update timestamp, update_freq text, status text, worker text, num_rows text)"
conn.execute_sql_command(sql)


sql = f"Select collection_name from {SYNC_LOCKER_TABLE_NAME} where update_freq = 'daily'"
conn.execute_sql_command(sql)
postgres_collections = []
for row in conn._cur.fetchall():
    postgres_collections.append([*row.values()][0])

nis_db = MongodbNISReadOnly()

mongo_collections = []

for collection_name in nis_db.collection_names():
    if collection_name not in postgres_collections:
        mock_dt = datetime(2022, 4, 20)
        mock_dt = mock_dt.strftime("%Y-%m-%d %H:%M:%S")
        query = f'insert into {SYNC_LOCKER_TABLE_NAME}("collection_name","last_update","update_freq","status","worker") values %s'
        conn.run_sql_bulk_execute(
            query, [(collection_name, mock_dt, "daily", "None", "None")])


# assign worker for all daily updated collection
# and update number of rows
sql = f'select collection_name from {SYNC_LOCKER_TABLE_NAME} order by num_rows'
conn.execute_sql_command(sql)
res = conn.fetch_all()

total_rows = 0
for i, row in enumerate(res):
    collection_name = [*row.values()][0]
    num_rows = nis_db[collection_name].count()
    total_rows += num_rows
    logger.debug(
        f"find collection in mongo: {collection_name}, number of rows: {num_rows}")
    query = f"update {SYNC_LOCKER_TABLE_NAME} set worker={i%NUM_WORKERS},num_rows={str(num_rows)},status='None' where collection_name = '{collection_name}';"
    conn.execute_sql_command(query)

logger.info(str(total_rows), labels={"metrics": "total_rows"})
