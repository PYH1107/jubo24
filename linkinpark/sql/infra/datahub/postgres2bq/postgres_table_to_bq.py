import pathlib
from linkinpark.lib.common.sql_loader import SqlLoader
from linkinpark.lib.common.bq_manager import BigQueryConnector

bq_connector = BigQueryConnector()


def execute(table_name=None, bq_dataset_name=None, pg_dataset_name=None):
    # To avoid unit test failed
    # TODO: need to upgrade to execute_bq_command.py supported
    if not table_name:
        return

    file_name = 'postgres_table_to_bq'
    file_path = str(pathlib.Path(__file__).parent.resolve())
    sql_path = file_path + "/" + file_name

    params = {"bq_dataset": bq_dataset_name,
              "pg_dataset": pg_dataset_name,
              "table": table_name}
    sql_loader = SqlLoader(sql_path, params)

    sql, _, __ = sql_loader.sql
    _, job = bq_connector.execute_sql_in_bq(sql)
    print(job.state)
