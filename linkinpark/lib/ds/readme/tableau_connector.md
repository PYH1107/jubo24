# Tableau Connector
A Jubo internal toolkit used to interact with Jubo's Tableau Server. Mainly 
used for downloading Tableau views.

## Outline
- [Features](#features)
- [Usage](#usage)
- [Dependencies](#dependencies)
- [Limitations](#limitations)

## Features
- Connect to Tableau Server via API.
- Query for views published on Tableau server.
- Create, update and delete data extract of views. <br>
  *(This feature was added due to a [SEV](https://docs.google.com/document/d/1Uasb4QRjBmzVb-ofnFyLtOFX9DalDvg8FcTpg_5z_0k/edit?usp=sharing))* 

## Usage
1. Connect to Jubo's Tableau Server.
    * Connect to Tableau server while the credential is already exported to 
      the environment.
        ```python
        tableau = TableauConnector()
        ```
    * Connect to Tableau server by providing the path of credential.
        ```python
        tableau = TableauConnector("CREDENTIAL.json")
        ```
2. Query of views.
    * Query for all views.
        ```python
        tableau.show_views()
        ```
    * Query for views inside a specific workbook.
        ```python
        tableau.show_views("WORKBOOK_ID")
        ```
3. **(Crucial)** Create or update workbook's data extract. <br>
   *Note: Please refer to the [SEV](https://docs.google.com/document/d/1Uasb4QRjBmzVb-ofnFyLtOFX9DalDvg8FcTpg_5z_0k/edit?usp=sharing) 
   for details.*
    ```python
    tableau.update_extract("WORKBOOK_ID")
    ```
4. Download views from Tableau server.
    * Download it as PDF.
        ```python
        file_obj = tableau.download_view("VIEW_ID")
        ```
    * Download it as images.
        ```python
        file_obj = tableau.download_view("VIEW_ID", "image")
        ```
    
## Dependencies
- [Python-time - Time access and conversions](https://docs.python.org/3.7/library/time.html)
- [Python-json — JSON encoder and decoder](https://docs.python.org/3.7/library/json.html)
- [Python-os — Miscellaneous operating system interfaces](https://docs.python.org/3.7/library/os.html)
- [Tableau API Lib - Call any method seen in Tableau Server's REST API](https://pypi.org/project/tableau-api-lib/)

## Limitations
- The supported file type for downloading is quit limited *(for now only pdf 
  and images are supported)*. This limitation was due to the supported file 
  type of Tableau REST API. For those who wish to add new file types to 
  this toolkit, please refer to the [documentation](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api.htm)
  of Tableau REST API first.  