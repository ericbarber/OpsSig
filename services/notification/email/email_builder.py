# notification/email/email_builder.py

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class EmailBuilder:
    def __init__(self, sender_email):
        self.sender_email = sender_email

    def build_email(self, recipient, subject, body_html):
        msg = MIMEMultipart('alternative')
        msg['From'] = self.sender_email
        msg['To'] = recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(body_html, 'html'))
        return msg
