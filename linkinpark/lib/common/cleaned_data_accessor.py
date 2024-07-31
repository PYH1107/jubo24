from linkinpark.lib.common.bq_manager import BigQueryConnector
from linkinpark.lib.common.rawdata_accessor import CaringRawdataAccessor

DEFAULT_PROJECT_ID = 'jubo-ai'
DEFAULT_ENVIRONMENT = 'dev'


class CaringCleandataAccessor(CaringRawdataAccessor):
    """
    For fetching raw data from BQ.
    The origin data is in chinese and didn't fit to BQ,
    therefore, the whole schema is encoded and hardly be read directly.

    This tool is aiming to retrieve tables decoded.
    """

    def get_table(self, table_name, rows_limit=1000, rows_offset=None):
        """
        Get the table of cleaned data, with optional rows limit.

        args:
            table_name [str]: a string that indicates the target table.
            row_limit [int, None]: limited rows, infinite if None, default to 1000.
        return:
            the table in a DataFrame
        """

        sql = f"""
            SELECT * FROM `jubo-ai.app_{self.environment}_knowledgegraph.patientKG_{table_name}_clean`
        """

        if rows_limit is not None:
            sql += f" LIMIT {rows_limit}"

            # offset will only applied when limited
            if rows_offset is not None:
                sql += f" OFFSET {rows_offset}"

        df_result, _ = self._conn.execute_sql_in_bq(sql)
        return df_result

    def get_distinct_value(self, table_name):
        """
        Get all distinct get_distinct_value from clean data, helping to check the process of clean
        """
        distinct_list = []
        column_list_code = self._schema_map[self._schema_map.table_name == table_name]['column_code'].tolist()
        for x in column_list_code:
            sql = f"""
                SELECT DISTINCT {x} FROM `jubo-ai.app_{self.environment}_knowledgegraph.patientKG_{table_name}_clean`
            """ 
            df, _ = self._conn.execute_sql_in_bq(sql)
            distinct_value = df.values.reshape(1, -1).tolist()[0]
            distinct_list.append(distinct_value)
        distinct_dict = dict(zip(column_list_code, distinct_list))
        return distinct_dict
