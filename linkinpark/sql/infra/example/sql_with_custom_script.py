import pathlib
from re import S
from linkinpark.lib.common.sql_loader import SqlLoader
from linkinpark.lib.common.bq_manager import BigQueryConnector

from datetime import datetime as dt

today = dt.today()

# Customize BQ execution with python script.
# Need to implement "execute" function and
# execute_bq_command will auto detection the function
# and execute it.


def execute():
    file_name = 'sql_with_custom_script'
    file_path = str(pathlib.Path(__file__).parent.resolve())
    sql_path = file_path + "/" + file_name
    sql_loader = SqlLoader(sql_path)
    bq_connector = BigQueryConnector()
    _, query, params = sql_loader.sql
    print(query, params)
    params['today_2'] = str(today)
    _, job = bq_connector.execute_sql_in_bq(
        sql_loader.bind_query_params(query, params))
    print(job.state)


if __name__ == "__main__":
    execute()
