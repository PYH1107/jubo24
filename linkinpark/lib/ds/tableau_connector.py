import json
import os
import time
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import workbooks


class TableauConnector:
    """
    A module to interact with Jubo Tableau Server REST API by calling
    functions.

    Examples
    --------
    # Create a TableauConnector object with default credential.
    >>> tableau = TableauConnector()

    # Create a TableauConnector object with specific credential path.
    >>> tableau = TableauConnector("credential_path")
    """

    def __init__(self, credential=None) -> None:
        self.conn = self._connect(credential)

    @staticmethod
    def _auth(credential):
        """
        This function will look at the file path provided for credential.
        If the file path is not provided, it will use the default credential
        in OS environment.
        :param credential: The file path to credential for authentication.
        :return: information for authentication to the Tableau server.
        """
        auth = None
        if credential is None:
            try:
                credential = os.environ["TABLEAU_CREDENTIALS"]
            except KeyError:
                raise ConnectionAbortedError(
                    "Default credential not found, please specified the "
                    "credential path.")
        with open(credential, "r") as file_obj:
            try:
                auth = json.load(file_obj)
            except ValueError:
                raise ConnectionAbortedError(
                    f"{credential} is not a valid json file."
                )
        return auth

    def _connect(self, credential) -> TableauServerConnection:
        """
        This function will make a connection to the Tableau server for
        further operations.
        :param credential: The file path to credential for authentication.
        :return: A TableauServerConnection object.
        """
        connection = TableauServerConnection(self._auth(credential))
        connection.sign_in()
        return connection

    def _check_workbook_id(self, workbook_id):
        """
        This function will check if the given workbook id is valid.
        :param workbook_id: The workbook id to be checked
        :return: The validation status of the id
        """
        df = workbooks.get_workbooks_dataframe(self.conn)
        valid_id = workbook_id in df["id"].to_list()
        return valid_id

    def _check_view_id(self, view_id):
        """
        This function will check if the gevin view id is valid.
        :param view_id: The view id to be checked
        :return: The validation status of the id
        """
        df = workbooks.get_views_dataframe(self.conn)
        valid_id = view_id in df["id"].tolist()
        return valid_id

    def show_views(self, workbook_id=None):
        """
        This function will look for all views available on the site,
        and limit the search result to views under a workbook if the
        workbook_id is given.
        :param workbook_id: The workbook to search in.
        :return: All views found.

        Examples:
        ---------
        # To show all views available on the site.
        >>> tableau.show_views()
            |       id |       name |           contentUrl
        --- | -------- | ---------- | --------------------
          0 |  view_id |  view_name |  workbook/sheet/view

        # To show all views under a workbook.

        >>> tableau.show_views("workbook_id")
            |       id |       name |           contentUrl
        --- | -------- | ---------- | --------------------
          0 |  view_id |  view_name |  workbook/sheet/view
        """
        if workbook_id is None:
            views = workbooks.get_views_dataframe(self.conn)
        elif self._check_workbook_id(workbook_id):
            views = workbooks.get_views_for_workbook_dataframe(
                self.conn, workbook_id)
        else:
            raise ValueError("Not a valid workbook id.")
        return views[["id", "name", "contentUrl"]]

    def get_workbook_id_by_view_id(self, view_id):
        """
        This function is used to search for the workbook_id which a view
        belongs to.
        :param view_id: The view_id to search.
        :return: The workbook id

        Examples
        --------
        # To get the workbook id of the given view id.
        >>> tableau.get_workbook_id_by_view_id("view_id")
        'workbook_id'
        """
        view_info = self.conn.query_view(view_id).json()
        if "view" in view_info:
            workbook_id = view_info["view"]["workbook"]["id"]
        else:
            raise ValueError("Not a valid view id")
        return workbook_id

    def _query_extract_job(self, job_id):
        """
        For some unknown reason, Tableau REST API's built in method query
        job do not support querying a create extract job. Hence this
        function was built to serve this purpose. It will query all
        background jobs by the query jobs, and than look for the job id
        which fit to the input.
        :param job_id: The job id to search for.
        :return: The job information.
        """
        res = self.conn.query_job(job_id)
        return res.json()["job"]

    def _wait_job_done(self, job_id):
        """
        Most Tableau Server jobs run asynchronously, so this function
        take place when we need things to be done synchronously.
        :param job_id: The id of the job we wish to be done
        """
        retry_time = 0
        while retry_time < 10:
            time.sleep(60)
            job_info = self._query_extract_job(job_id)
            try:
                if job_info["finishCode"] == "0":
                    return job_info["extractRefreshJob"]["notes"]
                else:
                    retry_time += 1
            except KeyError:
                self._wait_job_done(job_id)
        raise TimeoutError("The job takes greater than 10 min to finished.")

    def update_extract(self, workbook_id) -> None:
        """
        This function will try to create a extract for the workbook,
        and update the extract if it already exist.
        :param workbook_id: The id of workbook to update extract file.
        """
        if not self._check_workbook_id(workbook_id):
            raise ValueError("Not a valid workbook id.")
        res = self.conn.create_extracts_for_workbook(workbook_id)
        time.sleep(5)
        if res.status_code == 202:
            job_id = res.json()["job"]["id"]
            self._wait_job_done(job_id)
        else:
            error_detail = res.json()["error"]["detail"]
            if "because it is already extracted." in error_detail:
                res = self.conn.update_workbook_now(workbook_id)
                result = self._wait_job_done(res.json()["job"]["id"])
                print(result)
            else:
                raise NotImplementedError(
                    f"{res.status_code}: {res.json()['error']['detail']}"
                )

    def delete_extract(self, workbook_id) -> None:
        if not self._check_workbook_id(workbook_id):
            raise ValueError("Not a valid workbook id.")
        res = self.conn.delete_extracts_from_workbook(workbook_id)
        if res.status_code == 400:
            raise NotImplementedError("No extract to delete.")

    def download_view(self, view_id, file_type="PDF", params=None):
        """
        This function is used to download a view as file. Before start
        downloading the file, it will first make a extract for the workbook
        to avoid over high frequenct making quering to the datasource.
        :param view_id: The id of the view to download.
        :param file_type: The file to download as.
        :param params: The params while downloading.
        :return: The downloaded file.

        Note
        ----
        Params for pdf: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#query_view_pdf
        Params for image: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#query_view_image

        Example
        -------
        # To download a view as PDF file.
        >>> result = tableau.download_view("view_id", param_dict)

        # To download a view as image file.
        >>> result = tableau.download_view("view_id", "image", param_dict)
        """
        if not self._check_view_id(view_id):
            raise ValueError("Not a valid view id.")
        if file_type == "PDF":
            download_function = self.conn.query_view_pdf
        elif file_type == "image":
            download_function = self.conn.query_view_image
        else:
            raise ValueError(f"Unrecognized file type {file_type}")

        try_times, status, file_download = 0, None, None
        while not status == 200:
            file_download = download_function(view_id, parameter_dict=params)
            status = file_download.status_code
            if try_times < 10:
                try_times += 1
            else:
                raise TimeoutError(
                    f"Failed for getting the {view_id} too many times."
                )
        return file_download
