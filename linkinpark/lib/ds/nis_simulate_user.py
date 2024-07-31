import mimetypes
import requests
from io import BytesIO


class NisSimulateUser:
    """
    A module use to log into Jubo NIS and act as a simulated user to write
    records.

    Examples
    --------
    # Create a NisSimulateUser object.
    >>> nis = NisSimulateUser("user_account", "user_password", "prod")
    """
    def __init__(self, user, password, env="aids"):
        self.user, self.password, self.env = user, password, env
        self.host = self._get_host()
        self.token = self._login()
        self.header = {
            "authorization": self.token,
            "user-agent": "Jubo AIDS-Team / Datarch worker pipeline."
        }

    def _get_host(self):
        """
        This is an internal function which will determine the API host
        address base on the environment settings.

        :return: The API host address.
        """
        if self.env in ("aids", "dev"):
            host = "https://demo.jubo.health"
        elif self.env == "prod":
            host = "https://smc.jubo.health"
        else:
            raise ValueError(f"Unrecognized environment {self.env}.")
        return host

    @staticmethod
    def _get_path(module, form_id=None, patient_id=None):
        """
        This is an internal function used to generate the API path by the
        module and patient.

        :param module: The name of module you are working on.
        :param form_id: The customized form's id your are working on, only
        needed while working with customized forms.
        :param patient_id: The patient id of the patient or user (base on
        the type of customized form you are working on), only
        needed while working with customized forms.
        :return: The API path.
        """
        if module == "customizeForm":
            if form_id and patient_id:
                path = f"/api/{module}/{form_id}/patient/{patient_id}"
            else:
                raise ValueError(
                    "When module is customizeForm, form_id and patient_id "
                    "must be provided."
                )
        elif module == "login":
            path = "/api/jwt/auth/login"
        elif module == "fileCenter":
            path = "/api/file-gcs"
        else:
            path = f"/api/{module}?patientId={patient_id}"
        return path

    def _get_api_url(self, module, form_id=None, patient_id=None):
        """
        This is an internal function used to generate the full API path for
        making a request.

        :param module: The name of module you are working on.
        :param form_id: The customized form's id your are working on, only
        needed while working with customized forms.
        :param patient_id: The patient id of the patient or user (base on
        the type of customized form you are working on), only
        needed while working with customized forms.
        :return: The full API path for making a request.
        """
        url = self.host + self._get_path(module, form_id, patient_id)
        return url

    def _login(self):
        """
        An internal function used to login to Jubo NIS as an user base on
        the user account and password provided.

        :return: The API token for making further requests.
        """
        url = self._get_api_url("login")
        res = requests.post(
            url,
            {"username": self.user, "password": self.password}
        )
        res.raise_for_status()
        token = res.json()["token"]
        return token

    @staticmethod
    def _check_file_name(file):
        """
        An internal function used to check the file name of a file provided.
        If a file is loaded from localhost with the python built in open
        method, usually the file name is already part as the file information.
        In such case this function will try to get that file name and pass
        it to other functions for future usage.

        :param file: The file to checked.
        :return: The file name detected.
        """
        try:
            file_name = file.name
        except AttributeError:
            raise ValueError(
                "Cannot detect file name, hence parameter file_name must "
                "be provided."
            )
        return file_name

    def upload_file(self, file, file_name=None, content_type=None):
        """
        This function is used to upload a file to Jubo NIS Google Cloud
        Storage. The function will return a file id, which will be needed to
        allocate the file with some other record data.

        :param file: The file to upload.
        :param file_name: The file name of the file.
        :param content_type: The file's content type.
        :return: The file id of the uploaded file.
        """
        if not file_name:
            file_name = self._check_file_name(file)
        if not content_type:
            content_type = mimetypes.guess_type(file_name)
        if isinstance(file, BytesIO):
            file = file.getbuffer()
        url = self._get_api_url("fileCenter")
        res = requests.post(
            url,
            headers=self.header,
            files={"file": (file_name, file, content_type)})
        res.raise_for_status()
        file_id = res.json()["_id"]
        return file_id

    def write_record(self, module, data, form_id=None, patient_id=None):
        """
        This function is used to create a new record on Jubo NIS.

        :param module: The name of the module to create a new record.
        :param data: The data of the record (in JSON format).
        :param form_id: The customized form's id your are working on, only
        needed while working with customized forms.
        :param patient_id: The patient id of the patient or user (base on
        the type of customized form you are working on), only
        needed while working with customized forms.
        :return: The response of the request.
        """
        url = self._get_api_url(module, form_id, patient_id)
        res = requests.post(url, headers=self.header, json=data)
        res.raise_for_status()
        return res
