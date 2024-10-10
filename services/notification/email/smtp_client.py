# notification/email/smtp_client.py

import smtplib

class SMTPClient:
    def __init__(self, config):
        self.config = config

    def send_email(self, recipient, msg):
        if self.config.use_ssl:
            with smtplib.SMTP_SSL(self.config.smtp_server, self.config.smtp_port) as server:
                server.login(self.config.username, self.config.password)
                server.sendmail(self.config.username, recipient, msg.as_string())
        else:
            with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                server.starttls()
                server.login(self.config.username, self.config.password)
                server.sendmail(self.config.username, recipient, msg.as_string())
