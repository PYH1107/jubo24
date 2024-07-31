import logging
import os

import yaml
from jinjasql import JinjaSql

ROOT_PATH = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), '../../../')

# same as JUBO_WORKSTATE in datarch
GLOBAL_PARAMS = {
    'DATARCH_ENV': 'dev' if os.environ.get("DATARCH_CODESPACE", "default-dev").split('-')[-1] != 'prod' else 'prod'
}


class SqlLoader:
    def __init__(self, sql_path, params=None):
        if not os.path.exists(sql_path):
            sql_path = os.path.join(ROOT_PATH, sql_path)
        else:
            sql_path = sql_path

        self.sql_src = self.load_sql_file(sql_path)

        if params:
            self.params = params
        else:
            self.params = self.load_param_file(sql_path)

        # insert global variables
        self.params.update(GLOBAL_PARAMS)

        logging.info(f'load sql:{sql_path} with params:{self.params}')

        self.jinja = JinjaSql(param_style='pyformat')

    @property
    def sql(self):
        if self.params:
            query, bind_params = self.jinja.prepare_query(
                self.sql_src, self.params)
        else:
            query = self.sql_src
            bind_params = {}

        sql = self.bind_query_params(query, bind_params)
        return sql, query, bind_params

    def bind_query_params(self, query, bind_params):
        return query % (bind_params)

    def load_sql_file(self, sql_path):
        # find sql file via sql_name
        file_path = f'{sql_path}.sql'
        query_metadata = self._load_file(file_path)
        logging.debug(f"Loaded sql file: {sql_path}")
        return query_metadata

    def load_param_file(self, sql_path):
        # find params file via sql_name
        file_path = f'{sql_path}.yaml'
        try:
            yaml_data = self._load_file(file_path)
        except Exception:
            return {}
        param_dict = yaml.safe_load(yaml_data)
        logging.debug(f"Loaded yaml file: {file_path}")
        return param_dict

    def _load_file(self, file_path):
        try:
            with open(file_path, 'r') as file_data:
                return file_data.read()
        except Exception as e:
            logging.error(e, exc_info=True)
            raise
