# notification/email/service.py
import smtplib  
from .config import EmailConfig
from .template_renderer import TemplateRenderer
from .email_builder import EmailBuilder
from .smtp_client import SMTPClient

class EmailService:
    def __init__(self, config, template_dir):
        print(f"Template Directory: {template_dir}")
        self.config = config
        self.renderer = TemplateRenderer(template_dir)
        self.builder = EmailBuilder(config.username)
        self.smtp_client = SMTPClient(config)

    def send(self, recipient, subject, template_name, context):
        try:
            body_html = self.renderer.render(template_name, context)
            msg = self.builder.build_email(recipient, subject, body_html)
            self.smtp_client.send_email(recipient, msg)
            # Custom response object
            return {'status': 'success', 'details': 'Message sent'} 
        
        except smtplib.SMTPException as e:
            # Handle specific SMTP exceptions here
            return {'status': 'failure', 'error': str(e)}
