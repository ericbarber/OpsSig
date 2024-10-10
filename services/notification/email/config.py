# notification/email/config.py

class EmailConfig:
    def __init__(self, smtp_server, smtp_port, username, password, use_ssl=True):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
