# linkinpark

Jubo AI toolkit for all pipelines developing.

就是 ai/ds 團隊的 toolkit，提供 ai/ds 任何已開發之套件，所有 ai/ds 有可能會用到的共用物件都會開發在上面，上頭有相關的 coding style 檢查，以及 unit tests 撰寫要求，並且開發需要新增相對應的 readme文件，前人種樹後人乘涼。

## Outline
- [Installation](#installation)
- [Features](#features)
- [linkinpark目錄結構](#linkinpark-目錄結構)
- [AI Serving](#AI-Serving)
- [Execute SQL Commands in BQ via Python Scripts](#Execute-SQL-Commands-in-BQ-via-Python-Scripts)
- [FileEditor](#FileEditor)
- [相關閱讀](#相關閱讀)


## Installation
```
# latest version
pip3 install git+https://pip_installer:_xiWd4MyeY-6yMJpTLay@gitlab.smart-aging.tech/ds/infrastructure/linkinpark.git

# specific version
pip3 install git+https://pip_installer:_xiWd4MyeY-6yMJpTLay@gitlab.smart-aging.tech/ds/infrastructure/linkinpark.git@<commit-sha>
```
* *Debug*:
  * If following error message were presented while installing Linkinpark on
    Mac with M1 chips (this error message will not locate at the last line, 
    so one may need to scroll up and look for it in the console.), it means a 
    required library lssl for SSL and TSL is not found.
    ```
    <--Omit content--> 
    
      ld: library not found for -lssl
      clang: error: linker command failed with exit code 1 (use -v to see invocation)
      error: command '/usr/bin/gcc' failed with exit code 1
      [end of output]
    note: This error originates from a subprocess, and is likely not a problem with pip.
    ERROR: Failed building wheel for pymssql
    
    <--Omit content-->
    ```
    **Solution**
    * First step to try is to use the following command and install the 
      xcode-select.
      ```commandline
      xcode-select --install
      ```
      I some cases, after installing the XCode CLI you should be 
      able to install the Linkinpark package. However, if the following message 
      was returned for the command `xcode-select --install`, it mean your xcode
      CLI is working as it should be and it will not be the cause for the 
      failure. 
      ```commandline
      xcode-select: error: command line tools are already installed, use "Software Update" in System Settings to install updates
      ```
    * The next step for solving this problem will be manual install the openssl
      library, and export the openssl path to pip while installing.
      For person with homebrew installed, the following command will install 
      it for you.
      ```
      brew install openssl
      ```
      After installing the openssl, one still need to export the path so pip 
      may find it. Type the following command in terminal before you try to use
      pip install Linkinpark.
      ```
      export LDFLAGS="-L/opt/homebrew/opt/openssl/lib"
      export CPPFLAGS="-I/opt/homebrew/opt/openssl/include"
      ```
      If this still cannot fix the issue, congratulations you have found the 
      new land. The next markdown block of this debugging section is there 
      waiting for you to fill it. 
    
  * If following error message were presented while installing Linkinpark on
    Mac with M1 chips (this error message will not locate at the last line, 
    so one may need to scroll up and look for it in the console.), it means a 
    required library OpenMP needs to be installed.
    ```
      *** Building project with Ninja...
      ninja: error: '/opt/homebrew/opt/libomp/lib/libomp.dylib', needed by '/private/var/folders/4g/f41_tq311h7fj586pj58lgc00000gn/T/pip-install-8kz_4x9f/lightgbm_19bdde9a8bf94f83aeb725b7f1c4d1d8/lib_lightgbm.so', missing and no known rule to make it
      
      *** CMake build failed
      [end of output]
  
    note: This error originates from a subprocess, and is likely not a problem with pip.
    ERROR: Failed building wheel for lightgbm
    ```
    **Solution**
    Install the library OpenMP which is required by lightgbm. For homebrew 
    users, the following command should do it for you.
    ```
    brew install libomp
    ```
## Features
* **各種 Connector**
    * [Mongo Manager](test/lib/common/test_mongo_connector.py)
    * [BigQuery Manager](test/lib/common/test_bq_manager.py)
    * [Pubsub Manager](test/lib/common/test_pubsub_manager.py)

* [**撰寫符合架構之 SQL scripts 並執行於 BigQuery**](#execute-sql-commands-in-bq-via-python-scripts)
    * You can use `lpsql <absolute-path-to-your-sql>` after `pip install`. ( Note that you should use the absolute path ) 
      * ex. lpsql /workspace/datarch/pipelines/ai/sql/read_vitalsign_daily
    * `linkinpark/script/common/execute_bq_command.py` used to execute sql scripts which execute on BigQuery can be implemented sql query with or without parameter, and there were stored in `linkinpark/sql`.
    * The app could be used on any sql relational tasks, and be scheduled in datarch.

* **Served AI models**
    * [Activity Recommendation for NN](
      linkinpark/app/ai/activityRecommendationNN/README.md) - An AI model 
      built for NN project to predict the activity type for recording.
    * [Mental abnormal detection for NN](
      linkinpark/app/ai/mentalAbnormalNN/README.md) - An AI model built for NN 
      project to check if the mental score is abnormal for a specific period.
* **Data services**
    * [Report Platform Backend](
      linkinpark/app/ds/reportPlatformBackend/README.md) - The backend service
      for report platform.
* **Useful tools for data projects**
    * [Mail Sender](linkinpark/lib/ds/readme/mail_sender.md) - Used to send mail via SMTP services.
    * [NIS Simulate User](linkinpark/lib/ds/readme/nis_simulate_user.md) - Log in NIS and call API's via returned token.
    * [Sharedrive Connector](linkinpark/lib/ds/readme/sharedrive_connector.md) - Interacting with share drive via command line like method. 
    * [Tableau Connector](linkinpark/lib/ds/readme/tableau_connector.md) - Call Tableau server API with Python.

* **Other useful tools**
  * [GCS Helper](test/lib/common/test_gcs_helper.py)
  * [File Editor](test/lib/common/test_file_editor.py)

## linkinpark 目錄結構：
```
linkinpark/
	lib/
		common/
			bq_manager.py
			file_editor.py
		ai/
			
		ds/
			patients_feature_getter.py
	scripts/
		ai/
		ds/
		infra/<task>/README.md
	sql/
		ai/
		ds/
		infra/
	app/
		ai/
		ds/
		infra/
```
雖然於 linkinpark中有 scripts 跟 sql，但不是所有的 scripts 都要放於 linkinpark 中，而是「共用物件」才會被放到這裡。

## AI Serving
在和丹丹討論後，我們考慮將非 NN 類型 model 加入 linkinpark，原因不外乎其規模較小，也不像 NN 模型有許多共用物件可以拆解。以下也將根據納入 AI Serving 進行介紹。

<details><summary>Vitalsign Abnormal Detection</summary>
演算法：Isolation Forest<br>
Input：SYS / DIA / PR / SPO2 / TP<br>
Output：label / importantItems / rule<br>

服務主要以提供使用者上傳生命徵象後，針對其個人歷史資料及機構歷史資料來判斷該筆生命徵象有無異常。
* [Vitalsign Working Doc](https://docs.google.com/document/d/15jngkOFhZMGMKUmrFB24AgfQeNo9YG2nKlOX4aYp3WI/edit?usp=sharing)
* [Vitalsign Refactor Version](https://docs.google.com/document/d/1LZ-GF-EeSV6Uf2TTEvceZ-7RfXM7_7Y2oQn9onFmL10/edit?usp=sharing)

作為始祖級 Jubo AI，從開發至今大致可以分為三個版本：
1. 偉哲(開發者)版本：在日後AI用量因個案數大量成長，導致服務時常掛機
2. 鵬元(繼承者)版本：為了解決服務負載過高問題，使用多執行序去解決request塞車問題，同時也提高機器規格。雖然大量減低服務掛掉問題，但本身設計會無法釋出記憶體，在一段時間後仍會需要重啟服務。
3. Refactor 版本：此版本的開發有以下兩個最大原因
   1. 解決仍舊會服務需要重啟的問題
   2. 合併鎧琳後，其系統非屬 Jubo 架構內，此外，前兩個版本有無法及時回應 AI Result 的問題，為了再次解決服務不穩及即時回應兩個問題，本版將使用 Infra - Datarch 來解決上述問題。
4. Mapping Table 版本：為了將 Caring 資料倒入 datahub 且共同使用 AI Service，此次將所有生命徵象 ID 進行抽象化，並使用 mapping table 將 incoming request 的 organization & patient 進行轉換。

**Example usage:**

After using `pip install` linkinpark, try the following command to start AI service. 
```shell
run-ai-vitalsign
```
</details>


## Execute SQL Commands in BQ via Python Scripts
The script support three types of sql scripts:
1. **Only SQL**: 
    Only sql command without any params, just one \<filename\>.sql.
    example: src/sql/example/sql_without_params
2. **SQL with params**: 
    SQL with some parameters, which include \<filename\>.sql and \<filename.yaml\>
    you can set your parameters in yaml for SQL.
    example: src/sql/example/sql_with_params
3. **SQL with params and customize python**:
    In this type, you need to get some parameters by python scripts.
    so you can implement \<filename\>.py which include "execute" function
    execute_bq_command will auto load the function when the script be executed.
    example: src/sql/example/sql_with_custom_script

**Example usage:**
```
# setup for linkinpark and GCP
export PYTHONPATH=$PYTHONPATH:<path-to-linkinpark-dir>
export GOOGLE_APPLICATION_CREDENTIALS=<path-to-your-key>
```
<mark style="background-color: #35ad68">[New feature]</mark> Now we using the flag to determine what sql job to do, try _**lqsql -h**_ for detailed information
```commandline
usage: lpsql [-h] -s SQL_PATH [-b BUCKET_NAME] [-p BLOB_NAME]

Upload file to GCS

optional arguments:
  -h, --help            show this help message and exit
  -s SQL_PATH, --sql-path SQL_PATH
                        Path to your sql file
  -b BUCKET_NAME, --bucket-name BUCKET_NAME
                        The bucket name of gcs
  -p BLOB_NAME, --blob-name BLOB_NAME
                        The blob name(path) of gcs

```
```
python3 linkinpark/script/common/execute_bq_command.py -s <abs-path-to-sql> #(if test at local)
python3 linkinpark/script/common/execute_bq_command.py -s /<your-path>/linkinpark/linkinpark/sql/infra/example/sql_with_custom_script
```

## FileEditor 
This is a tool of file managment.
### **Current Features**:
1. #### **Basic files editing methods.**

    These methods helps managing files and directory, created with packages`os`and`shutil`.
    Basic file edit methods are written as `@staticmethod`.
    It's not necessary to instantiate `FileEditor()`.

    **Usage:**
    ```python
    from linkinpark.linkinpark.lib.common.file_editor import FileEditor
    FileEditor.create_dir('ex_path/ex_directory')
    FileEditor.create_file('ex_path/ex_file.txt')
    FileEditor.remove_file('ex_path/ex_file.txt')
    FileEditor.remove_dir('ex_path/ex_directory')
    ```
2. #### **Zip files managing methods.**

    There are three methods which helps you manage the `.zip` file easily:
    * `create_zip()`
    * `update_zip()`
    * `extract_zip()`

    **FileEditor must be instantiated while using these methods.**
    1. #### **create_zip:**
    
    Create a zip file which contains the source directory.

    **Usage:**
    ```python
    from linkinpark.linkinpark.lib.common.file_editor import FileEditor
    fe = FileEditor()
    fe.create_zip('source_path/source_directory', 'target_path/file.zip')
    # FileEditor will collect all files(include dir) in your source directory 
    ```
    2. #### **update_zip:**
    Update existing zip file without extracting it.
    
    ##### **a. Add new files to the zip file:**

    **Usage:**
    Create a new directory which is structured as the target zip file.
    *Structure of the example existing zip:*
    ```
    file.zip/
    example/
        ex/
            ex1.txt
        ex2.txt
    ```
    *Create the new \<example\> directory at local:*
    ```
    example/
        ex/
            ex_added.txt
    ```
    Run this
    
    ```python
    from linkinpark.linkinpark.lib.common.file_editor import FileEditor
    fe = FileEditor()
    fe.update_zip('source_path/source_directory', 'target_path/file.zip') 
    ```
    **Result:**
        *original zip file after updating*    
    ```
    file.zip/
    example/
        ex/
            ex1.txt
            ex_added.txt
        ex2.txt
    ```

    ##### **b. Replace existing files in the zipfile:**

    **Usage:**
    Create a new directory which is structured as the target zip file.
    *Structure of the example existing zip:*
    ```
    file.zip/
    example/
        ex/
            ex1.txt
        ex2.txt
    ```
    *Create the new \<example\> directory at local and edit the file that you want to replace:*
    ```
    example/
        ex/
            ex1.txt(edited)
    ```
    Run this

    ```python
    from linkinpark.linkinpark.lib.common.file_editor import FileEditor
    fe = FileEditor()
    fe.update_zip('source_path/source_directory', 'target_path/file.zip') 
    ```
    **Result:**
    *original zip file after updating*    
    ```
    file.zip/
    example/
        ex/
            ex1.txt(edited)
        ex2.txt
    ```
    3. **extract_zip**
    Extract existing zip file.
    This method is written as `@staticmethod`.
    It's not necessary to instantiate `FileEditor()`.

    **Usage:**
    ```python
    from linkinpark.linkinpark.lib.common.file_editor import FileEditor
    FileEditor.extract_zip('source_path/file.zip', 'target_path')
    ```
        
       
## 相關閱讀
* [linkinpark 的由來](https://docs.google.com/document/d/1F0laKDLTw4mG_TpieoWt-Dc37ji4UQkx0CNQKYiDJ4U/edit?usp=sharing)
* [linkinpark 的前身](https://gitlab.smart-aging.tech/ds/infrastructure/datahub)

