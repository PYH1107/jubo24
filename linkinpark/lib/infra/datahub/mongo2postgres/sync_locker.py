"""
Sync Locker is used to be lock for multiple syncing work
It's make system can sync multiple collection from mongo to postgres at same time.
"""
import os
from datetime import datetime

import pytz

from linkinpark.lib.common.postgres_connector import PostgresConnector, PostgresConnectorFactory

SYNC_LOCKER_DB_NAME = "mongo2postgres"
JUBO_WORKSTATE = os.environ.get("JUBO_WORKSTATE", "test")
SYNC_LOCKER_TABLE_NAME = "sync_lock"


class SyncLocker:
    def __init__(self):
        self.conn = PostgresConnectorFactory.get_cloudsql_postgres_connector(
            dbname=SYNC_LOCKER_DB_NAME, mode=JUBO_WORKSTATE)

    def lock_collection(self, collection_name):
        sql = f"update {SYNC_LOCKER_TABLE_NAME} set is_lock = True "\
            f"where collection_name='{collection_name}'"
        self.conn.execute_sql_command(sql)
        self.conn.commit()

    def release_collection(self, collection_name):
        sql = f"update {SYNC_LOCKER_TABLE_NAME} set is_lock = False "\
            f"where collection_name='{collection_name}'"
        self.conn.execute_sql_command(sql)
        self.conn.commit()

    def finish_collection(self, collection_name):
        now_dt = datetime.now(pytz.timezone("Asia/Taipei"))
        now_dt = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        sql = f"update {SYNC_LOCKER_TABLE_NAME}"\
            f" set is_lock = False ,last_update='{now_dt}' "\
            f"where collection_name='{collection_name}'"
        self.conn.execute_sql_command(sql)
        self.conn.commit()

    def search_locked_collections(self):
        sql = f"select collection_name from {SYNC_LOCKER_TABLE_NAME} "\
            f"where is_lock = True"
        self.conn.execute_sql_command(sql)

        result = []
        for row in self.conn._cur.fetchall():
            result.append([*row.values()][0])

        return result

    def search_expired_syncing_collections(self, due="daily"):
        """
        Used to search collection which needed to be update

        Args:
            due: Support "daily","weekly"
        """

        now_dt = datetime.now(pytz.timezone("Asia/Taipei"))

        sql = "select collection_name,last_update from sync_lock "\
            f"where is_lock = False and update_freq = '{due}'"
        self.conn.execute_sql_command(sql)
        result = []
        for row in self.conn._cur.fetchall():
            collection_name, last_update = [*row.values()]

            if due == "daily":
                if last_update.day != now_dt.day:
                    result.append(collection_name)
            elif due == "weekly":
                if last_update.isocalendar()[1] != now_dt.isocalendar()[1]:
                    result.append(collection_name)

        return result
