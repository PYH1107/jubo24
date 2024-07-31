'''
# Use case
# Example of uploading a folder
>>> bucket_name = "jubo-ai-mlp"
>>> project_name = "bedsore-regression"
>>> upload_folder = "bedsore-regression"
>>> helper = GcsHelper()
>>> helper.upload_folder_to_bucket(bucket_name, project_name, upload_folder)

# Example of downloading a folder
>>> download_folder = "my-dataset"
>>> helper.download_folder_from_bucket(bucket_name, project_name, download_folder)
'''
import json
import pickle
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage
from datetime import datetime
import os
import glob


def pickle_dumps(data):
    return pickle.dumps(data, protocol=4)


def pickle_loads(data):
    return pickle.loads(data)


def json_dumps(data):
    return json.dumps(data, indent=4, sort_keys=True)


def json_loads(data):
    return json.loads(data)


def jpg_dumps(data):
    return data


def jpg_loads(data):
    return data


func_format = {
    "pkl": (pickle_dumps, pickle_loads),
    "json": (json_dumps, json_loads),
    "jpg": (jpg_dumps, jpg_loads),
}


class GcsHelper:
    def __init__(self):
        self.client = self._connect_to_gcs_client()

    def _connect_to_gcs_client(self):
        try:
            client = storage.Client()
        except DefaultCredentialsError as e:
            print("DefaultCredentialsError:", e)
            raise "need to setting right GOOGLE_APPLICATION_CREDENTIALS env variable to auth"

        return client

    def _get_file_format(self, path):
        file_format = path.split(".")[-1]
        return file_format

    def upload_file_to_bucket(self, bucket_name, blob_name, path_to_file, timeout=60):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(path_to_file, timeout=timeout)

    def upload_folder_to_bucket(self, bucket_name, blob_name, path_to_folder, timeout=60, file_name_start_idx=1):
        """
        Accommodate strange file name behaviors while uploading folders to bucket. 

        I don't know why default starting file name index is 1. It is what it is. -Tony
        """
        assert os.path.isdir(path_to_folder)
        for local_file in glob.glob(path_to_folder + '/**'):
            if not os.path.isfile(local_file):
                self.upload_folder_to_bucket(
                    bucket_name, blob_name + "/" + os.path.basename(local_file), local_file)
            else:
                remote_path = os.path.join(
                    blob_name, local_file[file_name_start_idx + len(path_to_folder):])
                self.upload_file_to_bucket(
                    bucket_name, remote_path, local_file, timeout)

    def download_file_from_bucket(self, bucket_name, blob_name, path_to_file):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(path_to_file)

    def download_folder_from_bucket(self, bucket_name, blob_name, path_to_folder, file_name_start_idx=1):
        """
        Accommodate strange file name behaviors while downloading folders from bucket. 

        I don't know why default starting file name index is 1. It is what it is. -Tony
        """
        blobs = self.client.list_blobs(bucket_name, prefix=blob_name)
        os.makedirs(path_to_folder, exist_ok=True)
        for blob in blobs:
            file_name = os.path.join(
                path_to_folder, blob.name.split(blob_name)[1][file_name_start_idx:])
            splitting = file_name.split('/')
            if len(splitting) > 0:
                os.makedirs('/'.join(splitting[0:-1]), exist_ok=True)
            self.download_file_from_bucket(bucket_name, blob.name, os.path.join(
                path_to_folder, blob.name.split(blob_name)[1][file_name_start_idx:]))

    def upload_by_string_to_bucket(self, bucket_name, blob_name, data, content_type=None):
        bucket = self.client.get_bucket(bucket_name)
        extension_file = '.' in blob_name
        if extension_file:
            file_format = self._get_file_format(blob_name)
            bucket.blob(blob_name).upload_from_string(
                func_format[file_format][0](data))
        else:
            bucket.blob(blob_name).upload_from_string(
                data, content_type=content_type)

    def download_by_string_from_bucket(self, bucket_name, blob_name):
        bucket = self.client.get_bucket(bucket_name)
        file_format = self._get_file_format(blob_name)
        blob = bucket.get_blob(blob_name)
        dataset = func_format[file_format][1](blob.download_as_string())

        return dataset

    def get_blobs_list(self, bucket_name, bucket_prefix):
        blobs = []
        bucket = self.client.get_bucket(bucket_name)
        for blob in bucket.list_blobs(prefix=bucket_prefix):
            blobs.append(blob.name)

        return blobs

    def rename_blob(self, bucket_name, blob_name, new_blob_name):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        bucket.rename_blob(blob, new_blob_name)

    def copy_blob(self, bucket_name, blob_name, new_bucket_name, new_blob_name):
        source_bucket = self.client.get_bucket(bucket_name)
        source_blob = source_bucket.get_blob(blob_name)
        destination_bucket = self.client.get_bucket(new_bucket_name)
        new_blob = destination_bucket.copy_blob(
            source_blob, destination_bucket, new_blob_name)

    def delete_blob(self, bucket_name, blob_name):
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        blob.delete()

    def check_blob_exists(self, bucket_name, blob_name):
        bucket = self.client.get_bucket(bucket_name)
        is_blob_exists = bucket.get_blob(blob_name)
        return is_blob_exists is not None
