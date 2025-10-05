import requests
import json

url = "http://127.0.0.1:5001/invocations"

payload = {
    "dataframe_records": [
        {"ciudad": "Barcelona", "fecha": "2025-10-18"},
        {"ciudad": "Barcelona", "fecha": "2025-10-19"},
        {"ciudad": "Barcelona", "fecha": "2025-10-20"}
    ]
}

headers = {"Content-Type": "application/json"}

response = requests.post(url, data=json.dumps(payload), headers=headers)

print(response.status_code)
print(response.json())
