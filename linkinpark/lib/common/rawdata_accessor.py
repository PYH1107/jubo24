from linkinpark.lib.common.bq_manager import BigQueryConnector

DEFAULT_PROJECT_ID = 'jubo-ai'
DEFAULT_ENVIRONMENT = 'prod'

# a saved SQL query on BQ that can do the same thing
# https://console.cloud.google.com/bigquery?sq=626765861020:6144dc03ccc54f699e01ba2dc7d25c2d


class CaringRawdataAccessor:
    """
    For fetching raw data from BQ.
    The origin data is in chinese and didn't fit to BQ,
    therefore, the whole schema is encoded and hardly be read directly.

    This tool is aiming to retrieve tables decoded.
    """

    def __init__(self, project_id: str = DEFAULT_PROJECT_ID, environment: str = DEFAULT_ENVIRONMENT):
        self._conn = BigQueryConnector()
        self.project_id = project_id
        self.environment = environment
        self.__schema_map = None
        self.__table_map = None

    @property
    def _schema_map(self):
        if self.__schema_map is None:
            self._get_schema_maps()
        return self.__schema_map
    
    @property
    def _table_map(self):
        if self.__schema_map is None:
            self._get_schema_maps()
        return self.__table_map

    def _get_schema_maps(self):
        """Private method, retrieve schema map from BQ."""
        self.__schema_map, _ = self._conn.execute_sql_in_bq(f"""
            SELECT table_name, table_code, column_name, column_code FROM `jubo-ai.raw_{self.environment}_datahub_caring._schema_map`
        """)
        self.__table_map = self.__schema_map[['table_name', 'table_code']].drop_duplicates() \
            .set_index('table_name').to_dict()['table_code']
    
    def get_table_name_code(self):
        return self._schema_map
        
    def get_table_list(self):
        """
        Get all tables of Caring NIS.

        return:
            all table names in a list
        """
        return [*self._table_map.keys()]

    def get_table(self, table_name, rows_limit=1000, rows_offset=None, **kwargs):
        """
        Get the table of Caring NIS, with optional rows limit.

        args:
            table_name [str]: a string that indicates the target table.
            row_limit [int, None]: limited rows, infinite if None, default to 1000.
        return:
            the table in a DataFrame

        kwargs: include time_start [str] ex:'2020-5-5', time_end [str] ex:'2022-6-6', time_key [str] ex:'評估時間'
        """

        # get table code by table name
        table_code = self._table_map[table_name]

        column_map = self._schema_map[self._schema_map.table_name == table_name][['column_name', 'column_code']] \
            .set_index('column_code').to_dict()['column_name']
        
        sql = f"""
            SELECT * FROM `jubo-ai.raw_{self.environment}_datahub_caring.{table_code}`
        """

        dict_column = {v: k for k, v in self.column_name_code_ref(table_code).items()}

        if kwargs:
            time_code = dict_column[kwargs['time_key']]
            sql += "where %s BETWEEN '%s' and '%s'" % (time_code, kwargs['time_start'], kwargs['time_end'])

        if rows_limit is not None:
            sql += f" LIMIT {rows_limit}"

            # offset will only applied when limited
            if rows_offset is not None:
                sql += f" OFFSET {rows_offset}"

        df_result, _ = self._conn.execute_sql_in_bq(sql)

        df_result = df_result.rename(columns=column_map)

        return df_result
    
    def get_column_list(self, table_name):
        """
        Get all column names of the table.

        args:
            table_name [str]: a string that indicates the target table.
        return:
            column names in a list
        """
        return self._schema_map[self._schema_map.table_name == table_name]['column_name'].tolist()
    
    def get_distinct_value(self, table_name):
        """
        Get all distinct get_distinct_value
        """
        table_code = self._table_map[table_name]
        distinct_list = []
        column_list_code = self._schema_map[self._schema_map.table_name == table_name]['column_code'].tolist()
        column_list = self._schema_map[self._schema_map.table_name == table_name]['column_name'].tolist()
        for x in column_list_code:
            sql = f"""
                SELECT DISTINCT {x} FROM `jubo-ai.raw_{self.environment}_datahub_caring.{table_code}`
            """ 
            df, _ = self._conn.execute_sql_in_bq(sql)
            distinct_value = df.values.reshape(1, -1).tolist()[0]
            distinct_list.append(distinct_value)
        distinct_dict = dict(zip(column_list_code, distinct_list))
        return distinct_dict

    def column_name_code_ref(self, table_code):
        """get the column code from column name"""
        df = self.get_table_name_code()
        df = df[df['table_code'] == table_code]
        ref_dict = {}
        for x in range(df.shape[0]):
            ref_dict[df.iloc[x, 3]] = df.iloc[x, 2]
        return ref_dict