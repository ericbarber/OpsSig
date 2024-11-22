#/config/notification.py
import os
import sys

# Add the root of the project directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(project_root)

from config.app import app_details 

notification_details = {
    "template_store": app_details['notification_tempaltes']
}

example_notification_details = {
    "template_store": app_details['notification_tempaltes'] + "/examples"
}
