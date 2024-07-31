# Standard libraries
import time
import warnings
from io import BytesIO

# Related third party libraries
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload


GOOGLE_FILE_TYPE = {
    "application/vnd.google-apps.document":
        "application/vnd.openxmlformats-officedocument.wordprocessingml"
        ".document",
    "application/vnd.google-apps.presentation":
        "application/vnd.openxmlformats-officedocument.presentationml"
        ".presentation",
    "application/vnd.google-apps.spreadsheet":
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.google-apps.drawing": "image/jpeg",
}
MIME_TYPE = {
    "excel": "application/vnd.openxmlformats-officedocument.spreadsheetml"
             ".sheet",
    "word": "application/vnd.openxmlformats-officedocument.wordprocessingml"
            ".document",
    "PowerPoint": "application/vnd.openxmlformats-officedocument"
                  ".presentationml.presentation",
    "pdf": "application/pdf",
    "txt": "text/plain",
}


class ShareDriveConnector:
    """
    A module to use google drive api in command line like method.

    Examples
    --------
    # Create a ShareDriveConnector object.
    >>> drive = ShareDriveConnector()
    """

    def __init__(
        self, d_id=None, d_name=None, f_id=None, f_name=None
    ) -> None:
        self.service = build("drive", "v3")
        self.d_id, self.d_name = self._assign_drive(d_id, d_name)
        self.f_id, self.f_name = self._assign_folder(f_id, f_name)

    def show(self):
        """
        Show all share drives available.
        :return: Share drives available.

        Examples
        --------
        # To show all authorized share folder.
        >>> drive.show()
            |         id |         name |      kind
        --- | ---------- | ------------ | ---------
          0 |  folder_id |  folder_name | file_type
        """
        result = self.service.drives().list().execute()
        result = pd.DataFrame(result.get("drives"))
        if result.empty:
            raise ValueError(
                "No available share drive to list, please add the service "
                "account's email address to the share drive before applying."
            )
        return result

    def use(self, drive_name):
        """
        Select which share drive to use.
        :param drive_name: The name of share drive to use.
        :return: None

        Examples
        --------
        # To use a authorized share folder for further applications.
        >>> drive.use("drive_name")
        drive_name >
        """
        drive_list = self.show()
        drive = drive_list[drive_list["name"] == drive_name]
        if len(drive) == 0:
            raise FileNotFoundError(f"No drive named as {drive_name}")
        drive_id = drive.iloc[0]["id"]
        name = drive.iloc[0]["name"]
        self.d_id = drive_id
        self.d_name = name
        self.f_id = drive_id
        self.f_name = name
        self._directory_warning()

    def root(self):
        """
        Return to the root directory of the share drive used.
        :return: None

        Examples
        --------
        # Returning to the root of the share folder.
        >>> drive.root()
        drive_name >
        """
        self._drive_check()
        self.f_id, self.f_name = self.d_id, self.d_name
        warnings.warn(
            "ShareDriveConnector.root() is deprecated and will be "
            "removed from ShareDriveConnector in a future version. "
            "Please use ShareDriveConnector.cd() with no input value"
            " instead.",
            FutureWarning,
            stacklevel=2
        )
        self._directory_warning()

    def ls(self, folder: str = None, max_return=100):
        """
        List all file in the selected folder, by default the current directory.
        :param folder: The folder to lost all files.
        :param max_return: This param is used to change the max result to
        return. This value must be between 1 and 1,000.
        :return: Dataframe of file inside the folder.

        Examples
        --------
        # List all file under current working directory.
        >>> drive.ls()
            |       name |       id |  mineType
        --- | ---------- | -------- | ---------
          0 |  file_name |  file_id | file_type

        # List all file under a specific folder.
        >>> drive.ls("folder_id")
            |       name |       id |  mineType
        --- | ---------- | -------- | ---------
          0 |  file_name |  file_id | file_type
        """
        self._drive_check()
        if folder is None:
            folder = self.f_id
        try:
            result = self.service.files().list(
                driveId=self.d_id,
                includeItemsFromAllDrives=True,
                corpora="drive",
                supportsAllDrives=True,
                pageSize=max_return,
                q=f"'{folder}' in parents and trashed = false"
            ).execute()
        except HttpError as error:
            raise FileNotFoundError(
                f"{error}\n"
                f"Unrecognized folder id {folder} provided."
            )
        result = pd.DataFrame(result.get("files"))
        cols = ["name", "id", "mimeType"]
        for col in cols:
            if col not in result.columns:
                result[col] = None
        if len(result) == max_return:
            warnings.warn(
                "The returned result has reached the amount of maximum "
                "returns. Some records might not be shown due to this limit.",
                UserWarning,
                stacklevel=2
            )
        return result[cols]

    def dir_exist(self, dir_name: str, folder: str = None) -> bool:
        """
        Check is a directory name already exist in the folder.
        :param dir_name: The name of directory to check.
        :param folder: The folder to be checked.
        :return: Whether the directory exist.

        Examples
        --------
        # Check if a directory exist in the current working directory.
        >>> drive.dir_exist("file_name_exist")
        True

        >>> drive.dir_exist("file_name_not_exist")
        False

        # Check if a directory exist in a specific folder.
        >>> drive.dir_exist("file_name", "folder_id")
        True

        Limitation
        --------
        Currently this method is not recursive, which means it will
        only check if the directory exist in the folder without
        checking the sub-folders under this folder.
        """
        self._drive_check()
        if folder is None:
            folder = self.f_id
        file_list = self.ls(folder)
        if dir_name in file_list["name"].tolist():
            return True
        else:
            return False

    def get_file_info(self, file_name: str, folder: str = None) -> (str, str):
        """
        Get the file id and mineType by file name.
        :param file_name: The name of file to search.
        :param folder: The folder to look in for.
        :return: File id and mineType of the file found

        Examples
        --------
        # Get the information of a file in current directory.
        >>> drive.get_file_info("file_name")
        ('file_id', 'file_mime_tpye')

        # Get the information of a file in a specific folder.
        >>> drive.get_file_info("file_name", "folder_id")
        ('file_id', 'file_mime_tpye')
        """
        self._drive_check()
        if folder is None:
            folder = self.f_id
        file_list = self.ls(folder)
        files = file_list[file_list["name"] == file_name]
        if len(files) == 0:
            raise FileNotFoundError("No such file or directory.")
        else:
            file_id = files.iloc[0]["id"]
            file_type = files.iloc[0]["mimeType"]
            return file_id, file_type

    def cd(self, path: str = None) -> str:
        """
        Change directory to the path provided.
        :param path: Directory to change to.
        :return: None

        Examples
        --------
        # Change to a directory direct under current directory.
        >>> drive.cd("folder_name")
        drive_name/folder_name >

        # Change to a directory multilevel under current directory.
        >>> drive.cd("folder_name/sub_folder_name/")
        drive_name/folder_name/sub_folder_name >

        # Go back to the root of the share drive.
        >>> drive.cd()
        drive_name >
        """
        self._drive_check()
        if path:
            path_list = path.split("/")
            for sep_path in path_list:
                self.f_id = self.get_file_info(sep_path)[0]
                self.f_name = self.f_name + "/" + sep_path
            self._directory_warning()
        else:
            self.f_id = self.d_id
            self.f_name = self.d_name
            self._directory_warning()

    def cwd(self) -> None:
        """
        Print out the current directory.
        :return: None

        Examples
        --------
        # Print the current working directory.
        >>> drive.cwd()
        drive_name/working_directory_name >
        """
        self._drive_check()
        self._directory_warning()
        return self.f_name

    def mkdir(self, path: str) -> None:
        """
        Create a new folder in the current directory.
        :param path: The name of folder to create.
        :return: None

        Examples
        --------
        # Create a folder under current working directory.
        >>> drive.mkdir("folder_name")
        """
        self._drive_check()
        path_list = path.split("/")
        repository = self.f_id
        for sep_path in path_list:
            if self.dir_exist(sep_path, repository):
                repository = self.get_file_info(sep_path, repository)[0]
                continue
            file_metadata = {
                'name': sep_path,
                'parents': [repository],
                'mimeType': 'application/vnd.google-apps.folder'
            }
            self.service.files().create(
                body=file_metadata,
                supportsAllDrives=True,
                fields='id'
            ).execute()
            retry = 0
            while not self.dir_exist(sep_path, repository):
                if retry < 10:
                    time.sleep(3)
                    retry += 1
                else:
                    raise TimeoutError()
            repository = self.get_file_info(sep_path, repository)[0]
        return None

    def download(self, file_name: str) -> BytesIO:
        """
        Download a file from the current directory by file name.
        :param file_name: The name of the file to download.
        :return: The file downloaded.

        Examples
        --------
        # Download a file in the current working directory.
        >>> drive.download("file_name")
        """
        self._drive_check()
        file_list = self.ls()
        if file_name not in file_list["name"].tolist():
            raise ValueError("No such file or directory")
        file_info = file_list[file_list["name"] == file_name].reset_index()
        file_id = file_info.at[0, "id"]
        file_type = file_info.at[0, "mimeType"]
        file = BytesIO()

        # Need to use a different method to download Google workspace
        # documents, and the mimeType of those file also needs to be changed.
        if file_type in GOOGLE_FILE_TYPE:
            file_type = file_type.replace(
                file_type,
                GOOGLE_FILE_TYPE[file_type],
            )
            request = self.service.files().export_media(
                fileId=file_id,
                mimeType=file_type,
            )
        else:
            request = self.service.files().get_media(fileId=file_id)

        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        return file

    def upload(self,
               file_to_upload: BytesIO,
               file_name: str,
               file_type: str = "excel") -> None:
        """
        Upload an opened file to google drive.
        :param file_to_upload: The file to upload.
        :param file_name: The name of file to present on google drive.
        :param file_type: The file type of uploading file.
        :return: None

        MIME_TYPE
        ---------
        * excel
        * word
        * PowerPoint
        * pdf
        * txt

        Examples
        --------
        # Upload a Excel file to current working directory.
        >>> drive.upload("file_name")

        # Upload other file except excel to current working directory.
        # Note the file_type must be listed in the MIME_TYPE.
        >>> drive.upload("file_name", "file_type")

        """
        self._drive_check()
        self._directory_warning()
        if self.dir_exist(file_name):
            file_id, _ = self.get_file_info(file_name)
            media_body = MediaIoBaseUpload(
                file_to_upload,
                mimetype=MIME_TYPE[file_type],
            )
            self.service.files().update(
                fileId=file_id,
                supportsAllDrives=True,
                media_body=media_body,
            ).execute()
        else:
            file_metadata = {
                'name': file_name,
                'parents': [self.f_id],
            }
            media = MediaIoBaseUpload(
                file_to_upload,
                mimetype=MIME_TYPE[file_type],
            )
            self.service.files().create(
                body=file_metadata,
                supportsAllDrives=True,
                media_body=media,
                fields='id'
            ).execute()

    def delete(self, path: str, confirm=True) -> None:
        self._drive_check()
        self._directory_warning()
        try:
            folder, file_name = path.rsplit("/", 1)
            folder_id = self.get_file_info(folder)[0]
        except ValueError:
            folder_id, file_name = self.f_id, path
        file_list = self.ls(folder_id)
        if file_name not in file_list["name"].tolist():
            raise ValueError("No such file or directory")
        file_info = file_list[file_list["name"] == file_name].reset_index()
        file_name = file_info.at[0, "name"]
        file_id = file_info.at[0, "id"]
        if confirm:
            confirmed = input(
                f"Are you sure to delete the file {file_name}? [Y/N]"
            )
            if confirmed.upper() != "Y":
                print("Cancel deleting the file.")
                return None
        self.service.files().delete(
            fileId=file_id,
            supportsAllDrives=True
        ).execute()
        print(f"File {file_name} has been delete.")

    def _drive_check(self) -> None:
        """
        This is a internal function, used to check if the drive id is
        assigned and raise an error if the id is missing.
        :return: None
        """
        if self.d_id is None:
            raise FileNotFoundError(
                "No drive selected. Use method 'show' to present all"
                "available drives and use method 'use' to select a drive."
            )

    def _directory_warning(self) -> None:
        warnings.warn(
            f"\n Working directory: {self.f_name} > ",
            UserWarning,
        )

    def _assign_drive(self, d_id, d_name) -> (str, str):
        """
        This internal function will check the given drive id and drive name.
        If only id or name is given to the function, it will find the other one
        and add it to 'self.__init__()'. If both drive id and drive name was
        provided, this function will check if they matched.
        :param d_id: The id of share drive given.
        :param d_name: The name of share drive given.
        :return: The checked drive id and drive name.
        """
        df = self.show()
        if d_id is not None:
            name_by_id = df.loc[df["id"] == d_id].to_dict(orient="list")[
                "name"]
            if len(name_by_id) == 0:
                raise ValueError("The given drive id is not a valid id.")
            if d_name is not None:
                if name_by_id[0] != d_name:
                    raise ValueError("The drive id and name do not match.")
                else:
                    return d_id, d_name
            else:
                return d_id, name_by_id[0]
        elif d_name is not None:
            id_by_name = df.loc[df["name"] == d_name].to_dict(
                orient="list")["id"]
            if len(id_by_name) == 0:
                raise ValueError("The given drive name is not a valid name.")
            elif len(id_by_name) > 1:
                raise ValueError(
                    f"More than one result for the same drive name provided, "
                    f"please provide the id to specified which result to use."
                    f"\nNote: Use method 'show' to present available drives."
                )
            else:
                return id_by_name[0], d_name
        else:
            return None, None

    def _get_file_info_from_id(self, f_id):
        try:
            info = self.service.files().get(
                fileId=f_id,
                supportsAllDrives=True,
                fields="name, driveId, parents"
            ).execute()
        except HttpError as error:
            raise FileNotFoundError(
                f"{error}\n"
                f"Unrecognized folder id {f_id} provided."
            )
        return info

    def _assign_folder(self, f_id, f_name) -> (str, str):
        """
        This internal function will called another internal function
        'self._assign_id_and_name()' to check the folder id and folder name
        provided.
        :param f_id: The id of share drive given.
        :param f_name: The name of share drive given.
        :return: The checked drive id and drive name.
        """
        if f_id is not None or f_name is not None:
            self._drive_check()
        if self.d_id is None:
            return None, None
        name_from_id, id_from_name = None, None
        if f_id is not None:
            info_from_id = self._get_file_info_from_id(f_id)
            if self.d_id != info_from_id["driveId"]:
                raise IndexError("The f_id do not belong to the d_id.")
            name_from_id = info_from_id["name"]
            parents = info_from_id["parents"]
            if parents[0] == self.d_id:
                # name, _, _ = self._get_file_info_from_id(f_id).values()
                name = self._get_file_info_from_id(f_id)["name"]
                name_from_id = self.d_name + "/" + name
            else:
                while len(parents) != 0:
                    # name, parents, drive = self._get_file_info_from_id(
                    #     parents[0]).values()
                    parent_info = self._get_file_info_from_id(parents[0])
                    name, parents, drive = (
                        parent_info["name"],
                        parent_info["parents"],
                        parent_info["driveId"]
                    )
                    if parents[0] == drive:
                        name_from_id = (
                            self.d_name + "/" + name + "/" + name_from_id
                        )
                        parents = []
                    else:
                        name_from_id = name + "/" + name_from_id
        if f_name is not None:
            path_list = f_name.split("/")
            if path_list[0] == self.d_name:
                path_list = path_list[1:]
            parents = self.d_id
            for sep_path in path_list:
                info_from_name = self.service.files().list(
                    driveId=self.d_id,
                    includeItemsFromAllDrives=True,
                    corpora="drive",
                    supportsAllDrives=True,
                    q=f"name = '{sep_path}' and '{parents}' in parents"
                ).execute()["files"]
                if len(info_from_name) == 1:
                    info_from_name = info_from_name[0]
                elif len(info_from_name) == 0:
                    raise FileNotFoundError("No file with the name given.")
                else:
                    raise ValueError(
                        "More than one file with same name, please use f_id "
                        "to specify."
                    )
                parents = info_from_name["id"]
            id_from_name = parents
        if name_from_id is not None and id_from_name is not None:
            if id_from_name != f_id:
                raise ValueError("The f_id does not match with the f_name.")
            else:
                return f_id, f_name
        elif name_from_id is not None and id_from_name is None:
            return f_id, name_from_id
        elif name_from_id is None and id_from_name is not None:
            return id_from_name, f_name
        else:
            return self.d_id, self.d_name
