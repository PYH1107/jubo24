# Mail Sender
This toolkit is built to assist you sending E-mails via <i>Simple Mail 
Transfer Protocols (SMTP)</i> in Python.

## Outline
- [Features](#features)
- [Usage](#usage)
- [Dependencies](#dependencies)
- [Limitations](#limitations)

## Features
- Connect to SMTP server (Gmail as default) via application account and 
  password.
- Write emails by providing body, subject and address via text.
- Add file attachment to the mail, currently only PDF and excel are supported.
- Send out your mail with simply command after providing all the contents.

## Usage
1. Connect to the SMTP server.
    * Connect to the default SMTP server. <br>
      *(The default SMTP server is Gmail, and the application password for 
      connecting to Gmail SMTP server is different to the password used for 
      logging in Gmail via user interface.)*
        ```python
        from linkinpark.lib.ds.mail_sender import MailSender
        
        mail = MailSender(ACCOUNT, PASSWORD)
        ```
    * Connect to other SMTP servers.<br>
      *(The port to connect my differs by SMTP server providers, please look 
      at the official document of the service provider for which port to 
      connect to.)*
        ```python
        from linkinpark.lib.ds.mail_sender import MailSender
        
        mail = MailSender(ACCOUNT, PASSWORD, HOST, PORT)
        ```
2. Write the mail content by providing the text.
    ```python
    receiver = "someone@example.com"
    subject = "The title of your mail."
    body = "Here goes the mail's content body."
    mail.write_mail(receiver, subject, body)
    ```
3. (Optional) Attach a file to your mail.
    * Attach a file saved in your hard disk.
        ```python
        local_file = open("path_to_the_file.xlsx", "rb")
        file_name = "the_file_name.xlsx"
        file_type = "excel"
        mail.add_attachment(local_file, file_name, file_type)
        ```
    * Attach a file from BytesIO.
        ```python
        from io import BytesIO
        import pandas as pd
        
        bytes_file = BytesIO()
        df = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["A", "B", "C"])
        df.to_excel(bytes_file)
        file_name = "the_file_name.xlsx"
        file_type = "excel"
        mail.add_attachment(local_file, file_name, file_type)
        ```
4. Send out your mail after all content provided.'
    ```python
    mail.send_mail()
    ```

## Dependencies
- [Python-smtplib - SMTP protocol client](https://docs.python.org/3.7/library/smtplib.html#module-smtplib)
- [Python-email - An email and MIME handling package](https://docs.python.org/3.7/library/email.examples.html)

## Limitations
- Currently, only `PDF` and `Excel` files are supported as attachment. One may 
  add the main and sub MIME type to the method `add_attachment` to enlarge 
  the supported file types.<br>
  *Example:*
  ```python
  class MailSender:
      ...
      def add_attachment(self, file, file_name, file_type):
          ...
          file_types = {
            ...
            "new_file_type": {"maintype": "main-type", "subtype": "sub-type"}
          }
  ```
- Attaching multiple files at once is currently not supported, but one may 
  achieve the same result by following steps.
  ```python
  file_1, file_2 = BytesIO(), BytesIO()
  mail.add_attachment(file_1, "file_1.xlsx", "excel")
  mail.add_attachment(file_2, "file_2.xlsx", "excel")
  ```
