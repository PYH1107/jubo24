# Share Drive Connector
This toolkit is used to interact with Google Share Drive API in a command 
line like way.

## Outline
- [Features](#features)
- [Usage](#usage)
- [Dependencies](#dependencies)
- [Limitations](#limitations)

## Features
- Display all authentic share drives.
- Assign the share drive to use.
- Display all files under a folder. (ls command in Bash)
- Display current working directory. (cwd command in Bash)
- Create a new folder. (mkdir command in Bash)
- Upload, download and delete files in share drive folders.

## Usage
1. Export the GCP service account's credentials.
    ```
    export GOOGLE_APPLICATION_CREDENTIALS="PATH_TO_CREDENTIAL"
    ```
2. Create the ShareDriveConnector Object.
    * Create the object without assigning which drive or folder as current 
      working directory.
        ```python
        drive = ShareDriveConnector()
        ```
    * Specifying the working directory while creating the object.
        ```python
        # Specifying the working directory by it's name.
        drive = ShareDriveConnector(d_name="DRIVE_NAME", f_name="FOLDER_NAME")
      
        # Specifying the working directory by it's id.
        drive = ShareDriveConnector(d_id="DRIVE_ID", f_id="FOLDER_ID")
      
        # Specifying the working directory by mixed method.
        drive = ShareDriveConnector(d_name="DRIVE_NAME", f_id="FOLDER_ID")
        # or
        drive = ShareDriveConnector(d_id="DRIVE_ID", f_name="FOLDER_NAME")
        ```
      *(Providing both name and id for drive and folder are not needed but 
      allowed. Just make sure the id and name do point to the same 
      directory, otherwise an error may be raised.)*
3. (Optional) Display all drives and assign which on to use. <br>
   *(Only needed if drive are not specified while creating the object.)*
    ```python
    # List all available drives for this service account.
    drive.show()
    # Assign which share drive to use.
    drive.use("SHARE_DRIVE_NAME")
    ```
4. Display the current working directory.
    ```python
    drive.cwd()
    ```
5. Display all files under a folder.
    * Display the files under current working directory.
        ```python
        drive.ls()
        ```
    * Display the files under a specific folder.
        ```python
        drive.ls("FOLDER_ID")
        ```
6. Change the current working directory.
    * Change to a directory direct under current directory.
        ```python
        drive.cd("FOLDER_NAME")
        ```
    * Change to a directory multilevel under current directory.
        ```python        
        drive.cd("folder_name/sub_folder_name/")
        ```
    * Go back to the root of the share drive.
        ```python
        drive.cd()
        ```
7. Create a new folder under current working directory.
    * Create a single folder under current working directory.
        ```python
        drive.mkdir("FOLDER_NAME")
        ```
    * Create a recursive folder under current working directory.
        ```python
        drive.mkdir("PARENT_FOLDER_NAME/CHILD_FOLDER_NAME")
        ```
8. Download a file from Google Share Drive.
    ```python
    drive.download("FILE_NAME")
    ```
9. Upload a file to Google Share Drive.
    ```python
    drive.upload(FILE, "FILE_NAME", "FILE_TYPE")
    ```
10. Deleting a file or folder on Google Share Drive.
    * Deleting a file located at current working directory.
        ```python
        drive.delete("FILE_NAME")
        ```
    * Deleting a file located inside a folder.
        ```python
        drive.delete("PARENT_FOLDER_NAME/FILE_NAME")
        ```
## Dependencies
- [Python-time - Time access and conversions](https://docs.python.org/3.7/library/time.html)
- [Python-io - Core tools for working with streams](https://docs.python.org/3.7/library/io.html)
- [Pandas - Python Data Analysis Library](https://pandas.pydata.org/pandas-docs/version/1.3/reference/index.html)
- [Google API Client - Google API Client Library for Python](https://pypi.org/project/google-api-python-client/)
- [Warnings - Warning control](https://docs.python.org/3.7/library/warnings.html)

## Limitations
- Currently, only drive name not drive id are supported for the "use" 
  method while assigning which drive to use. If ever the feature of using 
  method "use" with drive id is required, it may be easily achieved by 
  calling the internal method "_assign_drive".
- Only folder id is allowed for the "ls" method. This limitation is due to 
  Google Share Drive allowed multi-folders with the same name under the 
  same parent path. This feature will lead to ambiguous result while 
  presenting files by folder name, since one may not be sure which folder 
  of the same name should be presented.
- Only file id is supported while using the method "download". Download 
  file by file id was not added simply because no one ever ask for it. *(By 
  modifying the method download, this feature is possible is someone ever 
  needs it.)*
- Upload method currently only support uploading file to the current 
  working directory.
- Currently, only the following file types listed are supported while 
  uploading.
    * excel
    * word
    * PowerPoint
    * pdf
    * txt

  For the future version of this toolkit, it is strongly 
  recommended using the "mimetypes" package to enhance the supported file 
  types.
- Recursive deletion for method "delete" was not implemented. This feature 
  might be something that is good to have, but not implemented due to no 
  current need.