import os
import json
import uuid
from datetime import datetime, timezone, timedelta

from linkinpark.lib.common.postgres_connector import PostgresConnectorFactory
from linkinpark.lib.common.gcs_helper import GcsHelper
from linkinpark.lib.common.file_editor import FileEditor

"""
Example of how to use DatasetUploader:
    dataset_uploader = DatasetUploader()
    blob_name = dataset_uploader.compute_blob_name("MyDataset", "bedsore")
    dataset_uploader.upload_dataset("MyDataset", blob_name)

Note:
    compute_blob_name() is optional, you can define your own blob_name
"""


class DatasetUploader:
    def __init__(self):
        self.postgres_connector = PostgresConnectorFactory.get_cloudsql_postgres_connector(
            dbname="mlp", mode='prod')
        self.bucket_name = "jubo-ai-dataset"

    def upload_dataset(self, data_folder_path, blob_name):
        """
        Steps of upload_dataset():

            1. check gcloud key and get email 

            2. insert an upload record on Postgre

            3. create zip for the dataset folder under same directory 

            4. upload the zip file to GCS

            5. remove zip file
        """
        account = self._get_gcp_user_email()
        dataset_name = os.path.basename(data_folder_path)
        dataset_id, create_date_str = self._generate_dataset_id(dataset_name)
        gcs_path = f"gcs://{self.bucket_name}/{blob_name}"
        insert_values = [[dataset_id, dataset_name, account, gcs_path, create_date_str, '']]
        self._record_upload_in_postgre(insert_values)

        zip_path = data_folder_path + ".zip"
        file_editor = FileEditor()
        file_editor.create_zip(data_folder_path, zip_path, False)

        gcs_helper = GcsHelper()
        gcs_helper.upload_file_to_bucket(self.bucket_name, blob_name, zip_path)

        os.remove(zip_path)

        return dataset_id

    def compute_blob_name(self, data_folder_path, subject_name):
        dataset_name = os.path.basename(data_folder_path)
        return f"{subject_name}/{dataset_name}.zip"

    def _generate_dataset_id(self, dataset_name):
        """
        Generate a unique dataset ID based on the dataset_name.

        Args:
            model_name (str): The name of the dataset.

        Returns:
            tuple: A tuple containing the generated dataset ID and the create_date as a string.
        """
        dt_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        dt_tw = dt_utc.astimezone(timezone(timedelta(hours=8)))

        create_date_str = dt_tw.strftime("%Y%m%d-%H%M%S")
        # Extract last 6 digits of UUID
        unique_id = str(uuid.uuid4().fields[-1])[:6]
        return f"{dataset_name}_{create_date_str}_{unique_id}", create_date_str

    def _record_upload_in_postgre(self, insert_values):
        insert_columns = [
            "dataset_id",
            "dataset_name",
            "account",
            "gcs_path",
            "create_date",
            "note"
        ]
        
        self.postgres_connector.create_table(
            table_name='datasets', column_names=insert_columns, dtypes=[str for _ in insert_columns])
        self.postgres_connector.insert_values(
            table_name="datasets", columns=insert_columns, values=insert_values)

    def _get_gcp_user_email(self):
        """
        Get the Google Cloud Platform user's email from the GOOGLE_APPLICATION_CREDENTIALS.

        Returns:
            str: The email of the user.

        Raises:
            ValueError: If GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.
        """
        # Check for GOOGLE_APPLICATION_CREDENTIALS environment variable
        credentials_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_file:
            raise ValueError(
                "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

        # Retrieve the user's email from the credentials file
        with open(credentials_file, "r") as f:
            credentials_json = f.read()

        credentials = json.loads(credentials_json)

        return credentials.get("client_email")
