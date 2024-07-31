from typing import Dict, List

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager

DEFAULT_PROJECT_ID = 'jubo-ai'
DEFAULT_ENCODING = 'UTF-8'


class SecretAccessor:
    """For easy access to SecretManager on GCP."""

    def __init__(self, project_id: str = DEFAULT_PROJECT_ID):
        self.client = self._connect_to_sms_client()
        self.project_id = project_id

    def _connect_to_sms_client(self):
        try:
            client = secretmanager.SecretManagerServiceClient()
        except DefaultCredentialsError as e:
            print("DefaultCredentialsError:", e)
            raise "need to setting right GOOGLE_APPLICATION_CREDENTIALS env variable to auth"
        return client

    @property
    def _project_path(self):
        return f'projects/{self.project_id}'

    def _secret_path(self, secret_id):
        return f'{self._project_path}/secrets/{secret_id}'

    def _version_path(self, secret_id, version_id):
        return f'{self._secret_path(secret_id)}/versions/{version_id}'

    def list_secrets(self) -> List[str]:
        from pathlib import Path
        return [Path(secret.name).name for secret in self.client.list_secrets(parent=self._project_path)]

    def list_secret_versions(self, secret_id: str) -> List[str]:
        """List all version ids.

        Arguments:
            secret_id -- id of secret existed.

        Returns:
            List of verison ids.
        """
        from pathlib import Path
        return [Path(version.name).name for version in
                next(self.client.list_secret_versions(parent=self._secret_path(secret_id)).pages).versions]

    def create_secret(self, secret_id: str):
        """Create a secret in project.\n
        It's recommended that to use append_secret instead.

        Arguments:
            secret_id -- id of secret.

        Returns:
            response
        """
        return self.client.create_secret(
            self._RequestBuilder.create_secret(self._project_path, secret_id))

    def append_secret(self, secret_id: str, secret_content: str, auto_create: bool = True):
        """Append a secret string to secret.\n
        Will create a secret if secret is not existed and auto_create is true.

        Arguments:
            secret_id -- id of secret.
            secret_content -- content string of secret.

        Keyword Arguments:
            auto_create -- to create a secret if not existed. (default: {True})

        Returns:
            response
        """
        from google.api_core.exceptions import AlreadyExists

        if auto_create:
            try:
                self.create_secret(secret_id)
            except AlreadyExists as e:
                print(e)

        return self.client.add_secret_version(
            self._RequestBuilder.add_secret_version(self._secret_path(secret_id), secret_content))

    def access_secret(self, secret_id: str, version_id: str = 'latest'):
        """Retrive a secret string from a secret.

        Arguments:
            secret_id -- id of secret.

        Keyword Arguments:
            version_id -- id of version or 'latest' (default: {'latest'})

        Returns:
            the secret string
        """
        response = self.client.access_secret_version(
            name=self._version_path(secret_id, version_id))
        return response.payload.data.decode(DEFAULT_ENCODING)

    def delete_secret(self, secret_id: str):
        self.client.delete_secret(name=self._secret_path(secret_id))

    class _RequestBuilder():

        def __init__(self) -> None:
            raise NotImplementedError

        @staticmethod
        def create_secret(project_path: str, secret_id: str) -> Dict:
            return {
                "parent": project_path,
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }

        @staticmethod
        def add_secret_version(secret_path: str, data: str) -> Dict:
            return {
                "parent": secret_path,
                "payload": {"data": data.encode(DEFAULT_ENCODING)}
            }
