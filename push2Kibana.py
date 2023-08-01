import json
import os
import requests

# Kibana DevTools API endpoint
kibana_devtools_url = "http://myKibanaURL/api/console/proxy?path=_devtools/console"

# Your Elasticsearch username and password
username = "user"
password = "pwd"

# Your Kibana `kbn-xsrf` token (you can get this token from a Kibana request header or generate it programmatically if necessary)
kbn_xsrf_token = "'kbn-verion': '6.4.0'"


index_name = "user_balance_report_test_0728"

# Function to execute a query using Kibana's DevTools console


def execute_query(query):
    headers = {
        "Content-Type": "application/json",
        "kbn-version": "6.4.0",
        "kbn-xsrf": "reporting"
    }
    response = requests.post(
        kibana_devtools_url,
        data=query,
        headers=headers,
        auth=(username, password)
    )
    return response.json()

# Function to import data from a file


def import_data_from_file(file_path):
    with open(file_path, "r") as file:
        try:
            data = json.load(file)
            for entry in data:
                # Convert entry to JSON string
                entry_json = json.dumps(entry)
                # Elasticsearch query to index the data
                query = json.dumps({
                    "method": "POST",  # Include the method parameter here
                    "path": f"/{index_name}/_doc",
                    "body": entry_json
                })
                response = execute_query(query)
                print(response)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON from {file_path}: {e}")


# Directory containing log files
log_files_directory = "testUserTest"

# Process each log file in the directory
for filename in os.listdir(log_files_directory):
    file_path = os.path.join(log_files_directory, filename)
    import_data_from_file(file_path)