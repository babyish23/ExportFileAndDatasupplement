import requests

# Elasticsearch endpoint
es_url = "http://myElasticSearchURL:9200"

# Your Elasticsearch username and password
username = "user"
password = "pwd"

# Index name
index_name = "user_balance_report_test_0728"
# index_name = "gl_user"

# URL to retrieve data from the index
url = f"{es_url}/{index_name}/_search"

# Elasticsearch query to get all documents (you can customize the query as per your requirements)
query = {
    "query": {
        "match_all": {}
    }
}

# Send the request with basic authentication
# response = requests.get(url, json=query, auth=(username, password))
response = requests.get(url, json=query)

# Check the response
if response.status_code == 200:
    data = response.json()
    # Process the data as needed
    print(data)
else:
    print(f"Failed to get data. Status code: {response.status_code}")
    print(response)
