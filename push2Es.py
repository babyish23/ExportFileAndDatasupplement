import json
import os
import requests

# Elasticsearch节点的URL
es_url = "Elasticsearch.com:9200"

# 要导入的索引名称
index_name = "user_balance_report_test_0728"

# Your Elasticsearch username and password
username = "user"
password = "pwd"

# Check if the index exists and verify authentication
response = requests.get(f"{es_url}/{index_name}", auth=(username, password))
print("test connection status code : ", response.status_code)


# Function to convert numeric strings to appropriate types
def convert_numeric_types(data):
    for key, value in data.items():
        if isinstance(value, str) and value.replace(".", "").isdigit():
            if "." in value:
                data[key] = float(value)
            else:
                data[key] = int(value)
        elif value == "null":  # Handle null values
            data[key] = None


# Function to import data from a file
def import_data_from_file(file_path):
    dataSize = 0
    with open(file_path, "r") as file:
        try:
            data = json.load(file)
            for entry in data:
                for key, value in entry.items():
                    if key not in ['day', 'id', 'username', 'parentName']:
                        # Remove double quotes for every value
                        value = value.replace('"', '')
                        # print("remove : " + key + ", value : " + value)

                # Convert numeric strings to appropriate types
                convert_numeric_types(entry)

                # Send POST request with basic authentication
                response = requests.post(
                    f"{es_url}/{index_name}/{index_name}",
                    json=entry,
                    auth=(username, password)
                )
                dataSize = dataSize + 1
                print("POST connection closed ! add dataSize : ", dataSize)
                print(response.json())
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON from {file_path}: {e}")


# Directory containing log files
log_files_directory = "testUserTest"

# Process each log file in the directory
for filename in os.listdir(log_files_directory):
    file_path = os.path.join(log_files_directory, filename)
    import_data_from_file(file_path)
