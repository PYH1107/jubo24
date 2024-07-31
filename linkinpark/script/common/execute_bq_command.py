"""\
This script used for sql execution in bigquery.
The script support three types of sql scripts:
    1. Only SQL: 
        Only sql command without any params, just one <filename>.sql.
        example: src/sql/example/sql_without_params
    2. SQL with params: 
        SQL with some parameters, which include <filename>.sql and <filename.yaml>
        you can set your parameters in yaml for SQL.
        example: src/sql/example/sql_with_params
    3. SQL with params and customize python:
        In this type, you need to get some parameters by python scripts.
        so you can implement <filename>.py which include "execute" function
        execute_bq_command will auto load the function when the script be executed.
        example: src/sql/example/sql_with_custom_script

Example usage:
python3 app/execute_bq_command.py <path-to-sql>
python3 app/execute_bq_command.py src/sql/infra/example/sql_with_params
# also support execute multiple sql
python3 app/execute_bq_command.py 
    src/sql/infra/example/sql_with_custom_script 
    src/sql/infra/example/sql_with_params
"""
import argparse
import importlib
import logging
import os
import sys
from typing import Dict

from linkinpark.lib.common.bq_manager import BigQueryConnector
from linkinpark.lib.common.gcs_helper import GcsHelper
from linkinpark.lib.common.sql_loader import SqlLoader


def execute(sql_path, params: Dict = None, **kwargs):
    py_script_path = sql_path + '.py'
    # check if it has sql command with customize python script
    if os.path.exists(py_script_path):
        # load python script and load execute function in script
        module_name = "execute"
        spec = importlib.util.spec_from_file_location(
            module_name, py_script_path)
        # creates a new module based on spec
        module = importlib.util.module_from_spec(spec)

        # executes the module in its own namespace
        # when a module is imported or reloaded.
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # executes the module directly
        module.execute()
    else:
        # load only sql file or sql with yaml file
        sql_loader = SqlLoader(sql_path, params=params)
        bq_connector = BigQueryConnector()
        sql, _, __ = sql_loader.sql
        # pass to bq connector and execute it
        # if args for datarch to execute sql then upload to gcs
        if 'bucket_name' in kwargs and 'blob_name' in kwargs:
            gcs_helper = GcsHelper()
            df, _ = bq_connector.execute_sql_in_bq(sql)
            if df.empty:
                logging.error(
                    "No data existed of your query job, please check your sql file!")
            else:
                gcs_helper.upload_by_string_to_bucket(
                    kwargs['bucket_name'], kwargs['blob_name'], df)
        else:
            bq_connector.execute_sql_in_bq(sql)


def main():
    parser = argparse.ArgumentParser(description='Upload file to GCS')
    parser.add_argument('-s', '--sql-path', required=True,
                        help='Path to your sql file')
    parser.add_argument('-b', '--bucket-name', required=False,
                        help='The bucket name of gcs')
    parser.add_argument('-p', '--blob-name', required=False,
                        help='The blob name(path) of gcs')
    parser.add_argument('-a', '--sql-arguments', required=False, nargs='*', action=keyvalue,
                        help='Arguments to be rendered to jinja templates in sql, seperated with a white space, i.e., -a a=1 b=2')

    args = parser.parse_args()
    logging.debug(args)

    if args.blob_name and args.bucket_name:
        sql_path = args.sql_path
        execute(sql_path,
                bucket_name=args.bucket_name,
                blob_name=args.blob_name)
    else:
        sql_path = args.sql_path
        params = args.sql_arguments

        execute(sql_path, params)


# ref : https://www.geeksforgeeks.org/python-key-value-pair-using-argparse/
# create a keyvalue class
class keyvalue(argparse.Action):
    # Constructor calling
    def __call__(self, parser, namespace,
                 values, option_string=None):
        setattr(namespace, self.dest, dict())

        for value in values:
            # split it into key and value
            key, value = value.split('=')
            # assign into dictionary
            getattr(namespace, self.dest)[key] = value


if __name__ == "__main__":
    main()
