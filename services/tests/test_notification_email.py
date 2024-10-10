# notification/email/tests/test_service.py

from unittest.mock import patch, MagicMock
from services.notification.email.config import EmailConfig
from services.notification.email.service import EmailService
import smtplib  # Import this to raise SMTPException in tests

# Test case for sending email using SMTP_SSL
@patch('smtplib.SMTP_SSL')
def test_email_sending_smtp_ssl(mock_smtp_ssl):
    mock_smtp_instance = MagicMock()
    mock_smtp_ssl.return_value = mock_smtp_instance  # Return mock instance for SMTP_SSL

    # Mock the context manager methods
    mock_smtp_instance.__enter__.return_value = mock_smtp_instance
    mock_smtp_instance.__exit__.return_value = None

    # Mock the SMTP_SSL methods
    mock_smtp_instance.login.return_value = None
    mock_smtp_instance.sendmail.return_value = {}

    # Configuration for SSL
    config = EmailConfig(
        smtp_server="smtp.example.com",
        smtp_port=465,  # Correct port for SSL
        username="sender@example.com",
        password="password",
        use_ssl=True  # Ensure this is True for SSL
    )

    email_service = EmailService(config, template_dir="services/notification/email/templates")

    recipient = "recipient@example.com"
    subject = "Test Email"
    context = {"title": "Test Title", "message": "Test message content."}

    # Send the email
    email_service.send(
        recipient=recipient,
        subject=subject,
        template_name="notification_template.html",
        context=context
    )

    # Debug print to ensure the mock is correctly created
    print("Mock sendmail call count (SMTP_SSL):", mock_smtp_instance.sendmail.call_count)

    # Assert that login and sendmail were called
    assert mock_smtp_instance.login.called, "Expected 'login' to be called for SMTP_SSL."
    assert mock_smtp_instance.sendmail.called, "Expected 'sendmail' to be called for SMTP_SSL."
    mock_smtp_instance.sendmail.assert_called_once()
    sent_email = mock_smtp_instance.sendmail.call_args[0][2]
    
    assert "Test Title" in sent_email
    assert "Test message content." in sent_email

# Test case for sending email using SMTP_SSL and raising an SMTPException
@patch('smtplib.SMTP_SSL')
def test_email_sending_smtp_ssl_failure(mock_smtp_ssl):
    mock_smtp_instance = MagicMock()
    mock_smtp_ssl.return_value = mock_smtp_instance

    # Mock the context manager methods
    mock_smtp_instance.__enter__.return_value = mock_smtp_instance
    mock_smtp_instance.__exit__.return_value = None

    # Mock the SMTP_SSL methods to raise an SMTPException on sendmail
    mock_smtp_instance.sendmail.side_effect = smtplib.SMTPException("Failed to send email")

    # Configuration for SSL
    config = EmailConfig(
        smtp_server="smtp.example.com",
        smtp_port=465,  # Correct port for SSL
        username="sender@example.com",
        password="password",
        use_ssl=True  # Ensure this is True for SSL
    )

    email_service = EmailService(config, template_dir="services/notification/email/templates")

    recipient = "recipient@example.com"
    subject = "Test Email"
    context = {"title": "Test Title", "message": "Test message content."}

    # Send the email and capture the response
    response = email_service.send(
        recipient=recipient,
        subject=subject,
        template_name="notification_template.html",
        context=context
    )

    # Assert response indicates failure
    assert response['status'] == 'failure'
    assert "Failed to send email" in response['error']

# Test case for sending email using regular SMTP with STARTTLS
@patch('smtplib.SMTP')
def test_email_sending_smtp(mock_smtp):
    # Create the mock SMTP instance
    mock_smtp_instance = MagicMock()
    mock_smtp.return_value = mock_smtp_instance
    
    # Mock the context manager methods
    mock_smtp_instance.__enter__.return_value = mock_smtp_instance
    mock_smtp_instance.__exit__.return_value = None

    # Mock the SMTP methods
    mock_smtp_instance.starttls.return_value = None
    mock_smtp_instance.login.return_value = None
    mock_smtp_instance.sendmail.return_value = {}

    # Configuration for STARTTLS (non-SSL)
    config = EmailConfig(
        smtp_server="smtp.example.com",
        smtp_port=587,  # Correct port for non-SSL with STARTTLS
        username="sender@example.com",
        password="password",
        use_ssl=False  # Ensure this is False for STARTTLS
    )

    email_service = EmailService(config, template_dir="services/notification/email/templates")

    recipient = "recipient@example.com"
    subject = "Test Email"
    context = {"title": "Test Title", "message": "Test message content."}

    # Send the email
    email_service.send(
        recipient=recipient,
        subject=subject,
        template_name="notification_template.html",
        context=context
    )

    # Assert that starttls, login, and sendmail were called
    assert mock_smtp_instance.starttls.called, "Expected 'starttls' to be called."
    assert mock_smtp_instance.login.called, "Expected 'login' to be called."
    assert mock_smtp_instance.sendmail.called, "Expected 'sendmail' to be called."
    mock_smtp_instance.sendmail.assert_called_once()
    sent_email = mock_smtp_instance.sendmail.call_args[0][2]
    
    assert "Test Title" in sent_email
    assert "Test message content." in sent_email

# Test case for sending email using regular SMTP with STARTTLS and raising an SMTPException
@patch('smtplib.SMTP')
def test_email_sending_smtp_failure(mock_smtp):
    # Create the mock SMTP instance
    mock_smtp_instance = MagicMock()
    mock_smtp.return_value = mock_smtp_instance
    
    # Mock the context manager methods
    mock_smtp_instance.__enter__.return_value = mock_smtp_instance
    mock_smtp_instance.__exit__.return_value = None

    # Mock the SMTP methods to raise an SMTPException on sendmail
    mock_smtp_instance.sendmail.side_effect = smtplib.SMTPException("Failed to send email")

    # Configuration for STARTTLS (non-SSL)
    config = EmailConfig(
        smtp_server="smtp.example.com",
        smtp_port=587,  # Correct port for non-SSL with STARTTLS
        username="sender@example.com",
        password="password",
        use_ssl=False  # Ensure this is False for STARTTLS
    )

    email_service = EmailService(config, template_dir="services/notification/email/templates")

    recipient = "recipient@example.com"
    subject = "Test Email"
    context = {"title": "Test Title", "message": "Test message content."}

    # Send the email and capture the response
    response = email_service.send(
        recipient=recipient,
        subject=subject,
        template_name="notification_template.html",
        context=context
    )

    # Assert response indicates failure
    assert response['status'] == 'failure'
    assert "Failed to send email" in response['error']
