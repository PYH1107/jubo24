import logging
import time

from google.api_core import exceptions
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

JOB_STATE_POLLING_INTERVAL = 5


class BigQueryConnector:
    def __init__(self, location="asia-east1"):
        self.location = location
        self.client = self._connect_to_bq_client()

    def _connect_to_bq_client(self):
        try:
            client = bigquery.Client(location=self.location)
        except DefaultCredentialsError as e:
            logging.critical("DefaultCredentialsError:", e)
            raise "need to setting right GOOGLE_APPLICATION_CREDENTIALS env variable to auth"
        return client

    def execute_sql_in_bq(self, sql, job_config=None):
        logging.debug(f"Execute sql:{sql}")
        job = self.client.query(sql, job_config=job_config)
        while True:
            query_job = self.client.get_job(job.job_id)
            logging.debug("Job {} is currently in state {}".format(
                query_job.job_id, query_job.state))

            if query_job.state != "RUNNING":
                exception_msg = query_job.exception()
                if exception_msg:
                    logging.error(f"SQL execution error: {sql}")
                    raise exception_msg
                break
            time.sleep(JOB_STATE_POLLING_INTERVAL)
        time.sleep(1)
        return job.to_dataframe(), query_job

    def upload_df(self, df, table_id, replace=False):
        if replace:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        else:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        return self.client.load_table_from_dataframe(df, table_id, job_config=job_config)

    @property
    def datasets(self):
        return list(self.client.list_datasets())

    def tables(self, dataset_id):
        return list(self.client.list_tables(dataset_id))

    def get_table_last_modified_time(self, table_path):
        """
        args:
            table_path<string>: <project_id>.<dataset_id>.<table_id>
        return:
            datetime<datetime>: datetime on last modified date (US-Time)
        """
        # table_path here is include <project_id>.<dataset_id>.<table_id>

        project_id, dataset_id, table_id = table_path.split('.')

        sql = "SELECT TIMESTAMP_MILLIS(last_modified_time)"\
            f"FROM `{project_id}.{dataset_id}.__TABLES__` where table_id = '{table_id}'"

        try:
            result, _ = self.execute_sql_in_bq(sql)

        # if database not exist, return Nones
        except exceptions.NotFound:
            logging.warning(f'Dataset: {dataset_id} is not found.')
            return None

        # table not exist, then return None
        if len(result) == 0:
            logging.warning(f'Table: {table_id} is not found.')
            return None

        dt = result['f0_'][0].to_pydatetime()
        return dt
