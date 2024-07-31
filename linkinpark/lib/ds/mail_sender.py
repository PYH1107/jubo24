import smtplib
from email.message import EmailMessage


class MailSender:
    """
    A module used to send mails through SMTP services.

    Examples
    --------
    # Create a MailSender object
    >>> mail = MailSender("mail_account", "application_password")

    # Write your mail
    >>> mail.write_mail("send_to_address", "mail_subject", "mail_content")

    # Send it
    >>> mail.send_mail()
    """

    def __init__(
            self,
            account: str,
            password: str,
            host: str = "smtp.gmail.com",
            port: str = "587"
    ):
        """

        :param account: The email account to send mails from.
        :param password: The password for that mail account.
        (For some SMTP services, this password might be different than the
         password you use while logging in by user interface.)
        :param host: The host of SMTP service. (Gmail as default)
        :param port: The port to connect to the SMTP service. (587 for Gmail)
        """
        self.account, self.password = account, password
        self.host, self.port, self.connected = host, port, False
        self.conn = self._login()
        self.msg = EmailMessage()

    def _login(self):
        """
        This is an internal function used to login to the SMTP mail server.
        :return: SMTP server connection.
        """
        smtp = smtplib.SMTP(self.host, self.port)
        smtp.starttls()
        smtp.login(self.account, self.password)
        self.connected = True
        return smtp

    def connect(self):
        """
        This function is used to reconnect to the mail server.
        :return: None
        """
        if self.connected:
            raise TypeError("Already connected, no need to connect it again.")
        self.conn.connect(self.host, self.port)
        self.conn.starttls()
        self.conn.login(self.account, self.password)
        self.connected = True

    def quit(self):
        """
        This function is used to disconnect to the mail server.
        :return: None
        """
        if not self.connected:
            raise TypeError("Not connected, cannot quit connection.")
        self.conn.quit()
        self.connected = False

    def write_mail(self, send_to: str, title: str = None, content: str = None):
        """
        This function will be used to add the content and address of a mail.
        :param send_to: The email address to send to, either string or list.
        :param title: The mail's subject.
        :param content: The content body of the mail.
        :return: None

        Notice
        ------
        Gmail's SMTP service (the default SMTP service for Jubo) do not
        support the modification of send from address, hence the modification
        of send from was not implemented in this module. However, this feature
        may be added in the future is required.
        """
        self.msg = EmailMessage()
        self.msg["from"] = self.account
        self.msg["to"] = send_to
        self.msg["subject"] = title
        if content is not None:
            self.msg.set_content(content)

    def add_attachment(self, file, file_name, file_type):
        """
        This function is used to add a attachment to the mail.
        :param file: The file to attached to the mail in BytesIO format.
        :param file_name: The file name, must add suffix. (eg. sample.xlsx)
        :param file_type: The file type, excel or pdf.
        :return: None

        Notice
        ------
        If the attachment type for your application was not supported,
        please feel free to contribute it to the module or contact the
        maintainer.
        """
        excel_subtype = "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        file_types = {
            "excel": {"maintype": "application", "subtype": excel_subtype},
            "pdf": {"maintype": "application", "subtype": "pdf"},
        }
        if file_type not in file_types:
            raise TypeError(f"The attachment file type {file_type} is "
                            f"currently not supported.")
        file.seek(0)
        binary_data = file.read()
        maintype, subtype = file_types[file_type].values()
        self.msg.add_attachment(
            binary_data, maintype, subtype, filename=file_name
        )

    def send_mail(self):
        """
        By calling this function, the mail you created will be sent.
        :return: None
        """
        self.conn.send_message(self.msg)
