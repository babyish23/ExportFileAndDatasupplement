import os
import json
from datetime import datetime

# Define a generator function to yield log entries one by one


def get_log_entries(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        for log_entry in file:
            yield log_entry.strip()


# Define a function to process a single log entry and extract the relevant data
def process_log_entry(log_entry):
    try:
        # Extract the JSON data from the log entry
        json_start = log_entry.find("{")
        json_data = log_entry[json_start:]

        # Parse the JSON data and extract the EsUserBalanceReport object
        parsed_json = json.loads(json_data)
        balance_report = parsed_json["message"].split(
            "balanceReport:")[-1].strip()

        # Remove "EsUserBalanceReport(" and ")"
        balance_report = balance_report.replace(
            "EsUserBalanceReport(", "").replace(")", "")

        # Convert the EsUserBalanceReport string to a dictionary
        balance_report_dict = dict(item.split("=")
                                   for item in balance_report.split(", "))

        # Convert date strings to timestamps
        date_fields = [
            "abcUpdateTime", "abcUpdateTime", "ggUpdateTime", "ggUpdateTime","lastUpdateTime"
        ]

        for field in date_fields:
            if balance_report_dict[field] != "null":
                # Parse the date string and convert to timestamp
                dt = datetime.strptime(
                    balance_report_dict[field], "%a %b %d %H:%M:%S GMT%z %Y")
                balance_report_dict[field] = int(dt.timestamp())

        return balance_report_dict
    except Exception as e:
        print("Error parsing log entry:", e)
        return None

# Define a function to export the balance reports to separate files


def export_balance_reports(balance_reports, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(balance_reports, f, indent=4)


# Function to remove double quotes from all fields except day、id、username、parentName
def remove_double_quotes(data):
    result_data = []
    for entry in data:
        new_entry = {}
        for key, value in entry.items():
            if key not in ['day', 'id', 'username', 'parentName']:
                if isinstance(value, str) and '"' in value:
                    value = value.replace('"', '')
                elif isinstance(value, int):
                    value = str(value)
            new_entry[key] = value
        result_data.append(new_entry)
    return result_data


# Specify the directory containing the input log files
input_directory = "exported_files"
# Specify the output directory for the exported files
output_directory = "formattedLogs"
os.makedirs(output_directory, exist_ok=True)

# Process each input log file
for filename in os.listdir(input_directory):
    input_file = os.path.join(input_directory, filename)
    balance_reports = []

    # Process each log entry in the input file
    for log_entry in get_log_entries(input_file):
        balance_report = process_log_entry(log_entry)
        if balance_report is not None:
            balance_reports.append(balance_report)

    # Export the balance reports to separate files in chunks of 100,000 entries per file
    chunk_size = 100000
    num_chunks = len(balance_reports) // chunk_size
    if len(balance_reports) % chunk_size != 0:
        num_chunks += 1

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = (i + 1) * chunk_size
        chunk_data = balance_reports[start_idx:end_idx]
        output_file = os.path.join(
            output_directory, f"{filename}_format_part.json")
        result_data = remove_double_quotes(chunk_data)
        export_balance_reports(result_data, output_file)
