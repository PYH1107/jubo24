import os
import uuid
import json
import argparse
from datetime import datetime, timezone, timedelta

from linkinpark.lib.common.postgres_connector import PostgresConnectorFactory
from linkinpark.lib.common.gcs_helper import GcsHelper
from linkinpark.lib.common.file_editor import FileEditor

"""
Example of how to use ModelUploader:
    model_uploader = ModelUploader()
    model_uploader.upload_model(
        model_path="/path/to/your/model.h5",
        model_name="MyModel",
        app_name="MyApp",
        source_code="/path/to/your/source_code.py",
        image="/path/to/your/image",
        account="user@gmail.com"
    )
    model_uploader.download_model(
        model_name="MyModel",
        app_name="MyApp",
        download_path="/path/to/your/model.h5"
    )
"""


class ModelUploader:
    def __init__(self):
        self.postgres_connector = PostgresConnectorFactory.get_cloudsql_postgres_connector(
            dbname="mlp", mode='prod')
        self.gcs_helper = GcsHelper()
        self.bucket_name = "jubo-ai-models"

    def get_gcp_user_email(self):
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

    def generate_model_id(self, model_name, app_name):
        """
        Generate a unique model ID based on the model_name and app_name.

        Args:
            model_name (str): The name of the model.
            app_name (str): The name of the app.

        Returns:
            tuple: A tuple containing the generated model ID and the create_date as a string.
        """
        dt_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
        dt_tw = dt_utc.astimezone(timezone(timedelta(hours=8)))

        create_date_str = dt_tw.strftime("%Y%m%d-%H%M%S")
        # Extract last 6 digits of UUID
        unique_id = str(uuid.uuid4().fields[-1])[:6]
        return f"{app_name}_{model_name}_{create_date_str}_{unique_id}", create_date_str

    def generate_gcs_path(self, model_id, model_path):
        """
        Generate the GCS path for storing the model file.

        Args:
            model_id (str): The unique model ID.
            model_path (str): The local path to the model file.

        Returns:
            tuple: A tuple containing the GCS path, the bucket name, and the blob name.
        """
        app_name, _, __ = model_id.partition('_')
        _, ext = os.path.splitext(model_path)
        blob_name = f"{app_name}/{model_id}{ext}"
        gcs_path = f"gcs://{self.bucket_name}/{blob_name}"
        return gcs_path, self.bucket_name, blob_name

    def check_valid_name(self, name):
        """
        Check if the name is valid (does not contain '_').

        Args:
            name (str): The name to be checked.

        Returns:
            bool: True if the name is valid, False otherwise.
        """
        return "_" not in name

    def check_valid_account(self, account):
        """
        Check if the account is valid.

        Args:
            account (str): The account to be checked.

        Returns:
            bool: True if the account is valid, False otherwise.
        """
        # for real user
        if "@jubo.health" in account:
            return True
        # for service account
        elif "@jubo-ai.iam.gserviceaccount.com" in account:
            return True
        # for pipelines
        elif "datarch:" in account:
            return True
        else:
            return None

    def upload_model(self, model_path, model_name, app_name, source_code=None, image=None, account=None):
        """
        Upload a model to GCS and store its information in the Postgres database.

        Args:
            model_path (str):
                The path to the model file on the local machine.
                If model_path is path of folder, upload_model will compress first
            model_name (str): The name of the model.
            app_name (str): The name of the app.
            source_code (str, optional): The path to the source code (default is None).
            image (str, optional): The path to the docker image (default is None).
            account (str, optional): The Google account for GCS upload (default is None).

        Returns:
            str: The generated model ID.
        """

        # If a model_path's type is folder
        # the object will be compressed before being uploaded.
        if os.path.isdir(model_path) and os.path.exists(model_path):
            file_editor = FileEditor()
            if model_path.endswith("/"):
                _model_path = model_path[:-1] + ".zip"
            else:
                _model_path = model_path + '.zip'
            file_editor.create_zip(model_path, _model_path, keep_parent_folder=False)
            model_path = _model_path

        if not self.check_valid_name(model_name):
            raise ValueError(
                "Invalid model_name. Model name cannot contain '_' character.")

        if not self.check_valid_name(app_name):
            raise ValueError(
                "Invalid app_name. App name cannot contain '_' character.")

        model_id, create_date_str = self.generate_model_id(
            model_name, app_name)

        # Check for GOOGLE_APPLICATION_CREDENTIALS environment variable and set account accordingly
        if not account:
            account = self.get_gcp_user_email()
            if not account:
                raise ValueError(
                    "Cannot get GCP user email. Please set GOOGLE_APPLICATION_CREDENTIALS.")

        if not self.check_valid_account(account):
            raise ValueError(
                "Invalid account. Should be <user>@jubo.health, datarch:<name_of_pipeline>")

        gcs_path, bucket_name, blob_name = self.generate_gcs_path(
            model_id, model_path)

        # Store model information in Postgres DB
        insert_columns = [
            "model_id",
            "model_name",
            "app_name",
            "source_code",
            "image",
            "account",
            "gcs_path",
            "create_date",
            "note"
        ]
        insert_values = [[model_id, model_name, app_name, source_code,
                         image, account, gcs_path, create_date_str, '']]
        # create_table(self, table_name, column_names, dtypes, constraints: dict = [])
        self.postgres_connector.create_table(
            table_name='models', column_names=insert_columns, dtypes=[str for _ in insert_columns])
        self.postgres_connector.insert_values(
            table_name="models", columns=insert_columns, values=insert_values)

        # Upload model file to GCS
        self.gcs_helper.upload_file_to_bucket(
            bucket_name, blob_name, model_path, timeout=60)

        return model_id
    
    def download_model_by_id(self, model_id, download_dir):
        """
        Download model from GCS to local based on model_id, if model is zip file then extract it into folder.

        Args:
            model_id (str): The unique model ID.
            download_dir (str):
                The directory to store the model file on the local machine.
            
        Returns:
            str: The path model store in local.
        """
        
        app_name, _, __ = model_id.partition('_')
        model_name = self.gcs_helper.get_blobs_list(self.bucket_name, f'{app_name}/{model_id}')[0]
        ext = os.path.splitext(model_name)[-1]
        
        # If model is zip file then extract into folder
        if ext == '.zip':
            download_path = os.path.join(download_dir, model_id)
            dest_name = os.path.join(download_dir, os.path.split(model_name)[-1])
            self.gcs_helper.download_file_from_bucket(self.bucket_name, model_name, dest_name)
            FileEditor.extract_zip(dest_name, download_path)
            os.remove(dest_name)
        else:
            download_path = os.path.join(download_dir, f'{model_id}{ext}')
            self.gcs_helper.download_file_from_bucket(self.bucket_name, model_name, download_path)
            
        return download_path
    
    def download_model(self, model_name, app_name, download_dir=None, model_id=None):
        """
        Download model from GCS to local, if model is zip file then extract it into folder.

        Args:
            model_name (str): The name of the model.
            app_name (str): The name of the app.
            download_dir (str, optional):
                The directory to store the model file on the local machine.
                Default is None, which means the model file will be stored in the current working directory.
            model_id (str): The unique model ID.
            
        Returns:
            str: The path model store in local.
        """
        
        if download_dir is None:
            download_dir = os.getcwd()
        elif not os.path.isdir(download_dir):
            raise ValueError('download_dir must be a directory.')
        
        # If model_id is not given, download the latest model
        if model_id is None:
            model_list = self.gcs_helper.get_blobs_list(self.bucket_name, f'{app_name}/{app_name}_{model_name}')
            if not model_list:
                raise ValueError('No models found for the given model_name and app_name.')
            latest_model = max(model_list)
            model_id = os.path.splitext(os.path.basename(latest_model))[0]
            
        return self.download_model_by_id(model_id, download_dir)


def main():
    parser = argparse.ArgumentParser(description="Model Upload Script")
    parser.add_argument("--model_path", type=str,
                        required=True, help="Path to the model file")
    parser.add_argument("--model_name", type=str,
                        required=True, help="Name of the model")
    parser.add_argument("--app_name", type=str,
                        required=True, help="Name of the app")
    parser.add_argument("--source_code", type=str, default=None,
                        help="Path to the source code (optional)")
    parser.add_argument("--image", type=str, default=None,
                        help="Path to the image (optional)")
    parser.add_argument("--account", type=str, default=None,
                        help="Google account for GCS upload (optional)")
    args = parser.parse_args()

    model_uploader = ModelUploader()
    model_uploader.upload_model(
        model_path=args.model_path,
        model_name=args.model_name,
        app_name=args.app_name,
        source_code=args.source_code,
        image=args.image,
        account=args.account,
    )


if __name__ == "__main__":
    main()
