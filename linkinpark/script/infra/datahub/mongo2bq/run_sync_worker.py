import os
import sys
import traceback
from datetime import datetime

import pytz

from linkinpark.lib.common.logger import Logger
from linkinpark.lib.common.bq_manager import BigQueryConnector
from linkinpark.lib.common.postgres_connector import PostgresConnectorFactory
from linkinpark.sql.infra.datahub.postgres2bq.postgres_table_to_bq import execute

from mongo2postgres import Mongo2Postgres


WORKER_ID = sys.argv[1]
if len(sys.argv) > 2:
    COLLECTION_NAME = sys.argv[2]
else:
    COLLECTION_NAME = None

DATASET_POSTFIX = os.environ.get("DATASET_POSTFIX", "test")
BQ_DATASET_NAME = f'raw_{DATASET_POSTFIX}_datahub_mongo'
PG_DATASET_NAME = f'mongo_{DATASET_POSTFIX}'

SYNC_LOCKER_DB_NAME = "mongo2postgres"
SYNC_LOCKER_TABLE_NAME = f"sync_table_{DATASET_POSTFIX}"

LOGGER_LABELS = {
    'datarch': 'infra_datahub_mongo2bq',
    'team': 'infra',
    'env': DATASET_POSTFIX,
    'app': 'datahub',
    'process': 'run_sync_worker',
}

logger = Logger(labels=LOGGER_LABELS)

sync_db = PostgresConnectorFactory.get_cloudsql_postgres_connector(
    dbname=SYNC_LOCKER_DB_NAME, mode=None)
bq_connector = BigQueryConnector()


def get_collections():
    collections = []
    sql = f"select collection_name from {SYNC_LOCKER_TABLE_NAME} where worker = '{str(WORKER_ID)}' and update_freq = 'daily' order by num_rows"
    sync_db.execute_sql_command(sql)
    res = sync_db.fetch_all()
    for row in res:
        collection_name = [*row.values()][0]
        collections.append(collection_name)
    return collections


def get_expect_num_rows(collection_name):
    # get expect number of rows

    sql = f"select num_rows from {SYNC_LOCKER_TABLE_NAME} where collection_name = '{collection_name}'"
    sync_db.execute_sql_command(sql)
    res = sync_db.fetch_all()
    sync_table_num_rows = int(list(res[0].values())[0])
    if DATASET_POSTFIX == "test" and sync_table_num_rows > 999:
        return 999
    return sync_table_num_rows


def update_status(collection_name, status):
    # update status in sync table

    now_dt = datetime.now(pytz.timezone("Asia/Taipei"))
    now_dt = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    query = f"update {SYNC_LOCKER_TABLE_NAME} set status='{status}',last_update='{now_dt}'  where collection_name = '{collection_name}';"
    sync_db.execute_sql_command(query)


def check_mongo2postgres(collection_name, expect_table_num_rows):
    logger.debug(f"checking {collection_name} in postgres db")

    target_db = PostgresConnectorFactory.get_cloudsql_postgres_connector(
        dbname=PG_DATASET_NAME, mode=None)

    # get actual number of rows
    sql = f'select count(*) from "{collection_name}"'
    target_db.execute_sql_command(sql)
    res = target_db.fetch_all()
    target_table_num_rows = int(list(res[0].values())[0])
    logger.debug(
        f"mongo: {expect_table_num_rows},postgres: {target_table_num_rows}")
    assert target_table_num_rows >= expect_table_num_rows
    logger.debug(f"checking {collection_name} in postgres db success!")


def check_postgres2bq(collection_name, expect_table_num_rows):
    logger.debug(f"checking {collection_name} in bigquery")
    sql = f"select count(*) from jubo-ai.`{ BQ_DATASET_NAME }.{ collection_name }`"
    res, _ = bq_connector.execute_sql_in_bq(sql)
    target_table_num_rows = int(res["f0_"][0])

    assert target_table_num_rows >= expect_table_num_rows
    logger.debug(f"checking {collection_name} in bigquery success!")


def run_sync_worker():
    if COLLECTION_NAME:
        collections = [COLLECTION_NAME]
    else:
        collections = get_collections()

    for collection_name in collections:
        logger.debug(f"sync collection:{collection_name}")
        expect_table_num_rows = get_expect_num_rows(collection_name)

        # skipping empty collection
        if expect_table_num_rows == 0:
            logger.debug("Empty skipping...")
            continue

        try:
            mongo2postgres_syncer = Mongo2Postgres()
            # mongo to postgres
            mongo2postgres_syncer.sync_mongo2postgres(collection_name)
        except Exception as e:
            # if column or table is different between last day and current state
            # Need to re-build columns
            if isinstance(e, sync_db.undefined_table_errors) or isinstance(e, sync_db.undefined_column_errors):
                mongo2postgres_syncer = Mongo2Postgres()
                mongo2postgres_syncer.sync_mongo2postgres(
                    collection_name, reset_column=True)
            else:
                logger.error(f"failed sync {collection_name}: {e}")
                logger.error(traceback.format_exc())
                update_status(collection_name, "failure")

        try:
            # check mongo to postgres
            check_mongo2postgres(collection_name, expect_table_num_rows)
            # postgres to bq
            execute(collection_name, BQ_DATASET_NAME, PG_DATASET_NAME)
            # check postgres to bq
            check_postgres2bq(collection_name, expect_table_num_rows)
            # update status
            update_status(collection_name, "success")
            logger.info(str(expect_table_num_rows),
                        labels={"metrics": "sucess_rows"})
        except Exception as e:
            logger.error(f"failed sync {collection_name}: {e}")
            logger.error(traceback.format_exc())
            update_status(collection_name, "failure")
            logger.info(str(expect_table_num_rows),
                        labels={"metrics": "failure_rows"})


if __name__ == "__main__":
    run_sync_worker()
