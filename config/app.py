#/config/app.py
app_details = {
    "app_name": "OpsSig",
    "app_root": "/opssig",
    "app_schema": "cdo",
    "app_examples_schema": "examples_db",
    "spark_catalog": "/opt/spark/work-dir/opssig",
    "notification_tempaltes": "/opt/spark/work-dir/opssig/notification_templates"
}


if __name__ == "__main__":
# Iterate over the dictionary and print the name and path
    print("List Application Details")
    for key, value in app_details.items():
        # Unpack name and path from the inner dictionary
        print(f"\n{key}:{value}")
        
